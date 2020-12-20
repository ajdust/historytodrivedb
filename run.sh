onedrive --synchronize --download-only --single-directory 'History to Drive'
find "$HOME/OneDrive/History to Drive" -name "*.xlsx" | xargs -d '\n' historytodrivedb
