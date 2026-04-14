# Membership Data App

This app contains:

- membership/chapter import endpoint
- member lookup endpoint
- SQL-backed import staging helpers

Current migration source:

- `C:\Users\guruj\aga-functions\function_app.py`

Current trigger surface:

- `POST /api/import_memchap`
- `GET /api/lookup-members`

Migration note:

- category import currently remains helper logic used by mailbox ingestion
- helper code is still duplicated locally for safety and will be reduced later
