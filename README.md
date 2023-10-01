# IMAP Dedup
This script is intended to be used to deduplicate messages on an IMAP server that follows a Gmail-like structure.

For example, all messages are stored in the "All Mail" folder but mail with labels attached to it will show
up in additional IMAP folders.  This script can purge elements in an "All Mail" folder that have already been
labeled.

Emails are removed in a multi-step process, caching some information locally in a SQLite database to speed up
operations.

## Commands
The following commands should be run in the order documented in this table.

| Command               | Description |
| --------------------- | ----------- |
| pull                  | Pulls messages from IMAP server and loads in database |
| find-duplicates       | Scans the database for duplicates and marks them |
| print-duplicates      | Prints all duplicates along with message subject for allow hand spot-checking of data
| deduplicate           | Performs deduplication operation on IMAP server using duplicates found by `find-duplicates`. **WARNING:** This will delete messages on the server, ensure you have double-checked that the script is removing the right information

## Required Arguments
Before starting, you need to gather your IMAP credentials and determine the "All Mail" folder to deduplicate from

| Field                 | Description |
| --------------------- | ----------- |
| hostname              | Hostname of the IMAP server to connect to |
| username              | Username of the account in IMAP server to log into |
| password              | Password for the account in IMAP server to log into |
| all-mail              | The "All Mail" folder to remove messages from if duplicates found |