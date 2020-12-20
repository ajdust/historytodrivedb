use calamine::{open_workbook, DataType, Reader, Xlsx};
use chrono::prelude::*;
use postgres::{Client, NoTls};
use std::fmt;
use std::path::Path;

/// SQL to create the schema with tables to hold data for History to Drive
const CREATE_SCHEMA_SQL: &str = "\
    create schema if not exists history_to_drive;

    create table if not exists history_to_drive.history
    (
        history_id         serial        not null
            constraint history_pk primary key,
        timestamp          timestamp     not null,
        title              varchar(1000) not null,
        host               varchar(600)  not null,
        url                varchar(3000) not null,
        user_agent         varchar(3000) not null,
        origin_description varchar(100)  not null,
        origin_timestamp   timestamp     not null default now()
    );

    comment on table history_to_drive.history is 'Browser history from History To Drive';
    comment on column history_to_drive.history.timestamp is 'UTC datetime when the page was visited';
    comment on column history_to_drive.history.title is 'The document title of the page';
    comment on column history_to_drive.history.host is 'The window.location.host of the page';
    comment on column history_to_drive.history.url is 'The window.location.href of the page';
    comment on column history_to_drive.history.origin_description is 'Source file or author for the record';
    comment on column history_to_drive.history.origin_timestamp is 'UTC datetime when the record was inserted from the origin';

    create index if not exists history_to_drive_history_ix_origin_ts
        on history_to_drive.history (origin_description, timestamp);
    create index if not exists history_to_drive_history_ix_host_ts
        on history_to_drive.history (host, timestamp);
    create index if not exists history_to_drive_history_ix_ts
        on history_to_drive.history (timestamp);

    create table if not exists history_to_drive.tag
    (
        tag_id serial       not null
            constraint tags_pk
                primary key,
        tag    varchar(100) not null
    );

    comment on table history_to_drive.tag is 'Tags linked to browser history';
    create unique index if not exists history_to_drive_tags_tag_uindex
        on history_to_drive.tag (tag);

    create table if not exists history_to_drive.history_tag
    (
        history_id int not null
            constraint history_tag_history_id_fkey
                references history_to_drive.history,
        tag_id     int not null
            constraint history_tag_tag_id_fkey
                references history_to_drive.tag
    );

    comment on table history_to_drive.history_tag is 'Table to join tags to history';
    create unique index if not exists history_to_drive_history_tag_uindex
        on history_to_drive.history_tag (history_id, tag_id);";

/// SQL to insert history rows, insert tag rows, insert link from history to tags
const INSERT_HISTORY_ROW_SQL: &str = "\
    with record_insert_id as (
        insert into history_to_drive.history (timestamp, title, host, url, user_agent, origin_description)
            values ($1, $2, $3, $4, $5, $6)
            returning history_id
    )
       , tags_to_merge as (
        select tag
        from unnest($7::varchar[]) as t(tag)
    )
       , inserted_tags as (
        insert into history_to_drive.tag (tag)
            select tag
            from tags_to_merge
            where tag not in (select tag from history_to_drive.tag)
            returning tag_id
    )
       , tag_ids as (
        select tag_id
        from inserted_tags
        union
        select tag_id
        from history_to_drive.tag
        where tag in (select tag from tags_to_merge)
    )
    insert
    into history_to_drive.history_tag (history_id, tag_id)
    select r.history_id, t.tag_id
    from tag_ids t
        cross join record_insert_id r";

enum HistoryToDriveError {
    DeserializeError(calamine::DeError),
    ExcelError(calamine::XlsxError),
    CalamineError(calamine::Error),
    PostgresError(postgres::Error),
    Unexpected(String),
}

impl From<calamine::DeError> for HistoryToDriveError {
    fn from(error: calamine::DeError) -> Self {
        HistoryToDriveError::DeserializeError(error)
    }
}

impl From<calamine::XlsxError> for HistoryToDriveError {
    fn from(error: calamine::XlsxError) -> Self {
        HistoryToDriveError::ExcelError(error)
    }
}

impl From<calamine::Error> for HistoryToDriveError {
    fn from(error: calamine::Error) -> Self {
        HistoryToDriveError::CalamineError(error)
    }
}

impl From<postgres::Error> for HistoryToDriveError {
    fn from(error: postgres::Error) -> Self {
        HistoryToDriveError::PostgresError(error)
    }
}

impl fmt::Debug for HistoryToDriveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HistoryToDriveError::DeserializeError(de) => de.fmt(f),
            HistoryToDriveError::ExcelError(xlsx) => xlsx.fmt(f),
            HistoryToDriveError::CalamineError(cal) => cal.fmt(f),
            HistoryToDriveError::PostgresError(post) => post.fmt(f),
            HistoryToDriveError::Unexpected(s) => s.fmt(f),
        }
    }
}

fn create_schema(url: &str) -> Result<(), HistoryToDriveError> {
    let mut client = Client::connect(&url, NoTls)?;
    return client
        .batch_execute(CREATE_SCHEMA_SQL)
        .map_err(|e| HistoryToDriveError::from(e));
}

fn get_string(cell: &DataType) -> Result<String, HistoryToDriveError> {
    match cell {
        DataType::String(s) => Ok(s.clone()),
        DataType::Int(i) => Ok(format!("{}", i)),
        DataType::Float(f) => Ok(format!("{}", f)),
        DataType::Bool(b) => Ok(if *b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        DataType::Error(cell_error_type) => Err(HistoryToDriveError::Unexpected(format!(
            "Error: {}",
            cell_error_type
        ))),
        DataType::Empty => Ok("".to_string()),
    }
}

/// Read the Excel worksheet at the given path, and execute SQL with the given PostgreSQL URL
/// to insert history rows, insert tag rows, insert link from history to tags
fn insert_sheet(path: &String, origin: &str, url: &str) -> Result<i32, HistoryToDriveError> {
    let mut wb: Xlsx<_> = open_workbook(path)?;
    let range = wb
        .worksheet_range("Sheet1")
        .ok_or(calamine::Error::Msg("Cannot find 'Sheet1'"))??;

    let mut rows = range.rows().into_iter();
    let mut client = Client::connect(&url, NoTls)?;
    let mut p_origin = origin.to_string();
    p_origin.truncate(100);

    let last_ts = client
        .query_one(
            "\
            select coalesce(max(h.timestamp), '1970-01-01') last_ts
            from history_to_drive.history h
            where h.origin_description = $1",
            &[&p_origin],
        )
        .map_or(chrono::naive::MIN_DATETIME, |r| r.get("last_ts"));
    if last_ts.year() > 1970 {
        println!(
            "Max timestamp of {} found for {}, skipping records before then",
            last_ts, p_origin
        );
    } else {
        println!("No previous records found for {}", p_origin);
    }

    let mut count = 0;

    'runner: loop {
        let mut txn = client.transaction()?;
        let sql = txn.prepare(INSERT_HISTORY_ROW_SQL)?;

        while let Some(row) = rows.next() {
            // print!("{}, ", count);
            if &row.len() < &6 {
                return Err(HistoryToDriveError::Unexpected(format!(
                    "Only {} columns present",
                    &row.len()
                )));
            }

            let ts = get_string(&row[0])?;
            let tags = get_string(&row[1])?;
            // ignore #NAME errors that are possible for title
            let mut title = get_string(&row[2]).unwrap_or("".to_string());
            let mut host = get_string(&row[3])?;
            let mut url = get_string(&row[4])?;
            let mut ua = get_string(&row[5])?;

            if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&ts) {
                // Don't insert duplicate records, assumes timestamps from an origin only increase
                let pts: NaiveDateTime = timestamp.naive_utc();
                if pts <= last_ts {
                    continue;
                }

                title.truncate(1000);
                host.truncate(600);
                url.truncate(3000);
                ua.truncate(3000);
                let tags = tags
                    .split(";")
                    .filter(|t| t.chars().count() < 100)
                    .map(|t| t.trim())
                    .collect();
                let ptags = postgres_array::Array::from_vec(tags, 0);
                txn.execute(&sql, &[&pts, &title, &host, &url, &ua, &p_origin, &ptags])?;

                count += 1;
                if count % 1000 == 0 {
                    txn.commit()?;
                    continue 'runner;
                }
            }
        }

        txn.commit()?;
        break;
    }

    Ok(count)
}

/// Call with one or more file paths and POSTGRESQL_URL as an environment variable.
/// For instance: `find "$(pwd)" -name "*.xlsx" | xargs -d '\n' historytodrivedb`
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 1 {
        println!("Not enough arguments - expecting one or more paths to an Excel file");
        return;
    }

    for arg in &args {
        if !Path::new(arg).exists() {
            println!("Could not find file at {}", arg);
            return;
        }
    }

    let pg_url = std::env::var("POSTGRESQL_URL");
    match pg_url {
        Ok(pg) => match create_schema(&pg) {
            Ok(_) => {
                for path in &args {
                    if let Some(file_name) = Path::new(path).file_name().and_then(|n| n.to_str()) {
                        println!("Importing {} ...", &file_name);
                        match insert_sheet(path, &file_name, &pg) {
                            Ok(count) => println!("Done inserting {} history rows", count),
                            Err(v) => println!("{:?}", v),
                        }
                    }
                }
            }
            Err(e) => println!("Could not create schema {:?}", e),
        },
        Err(_) => println!("Could not find environment variable 'POSTGRESQL_URL'"),
    }
}
