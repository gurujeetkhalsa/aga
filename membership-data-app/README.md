# Membership Data App

This app contains:

- membership/chapter import endpoint
- member lookup endpoint
- TD list publishing endpoints
- SQL-backed import staging helpers

Current migration source:

- `C:\Users\guruj\aga-functions\function_app.py`

Current trigger surface:

- `POST /api/import_memchap`
- `GET /api/lookup-members`
- `GET /api/GenerateTDListA`
- `GET /api/GenerateTDListB`
- `GET /api/GenerateTDListN`
- `GET /api/tda`
- `GET /api/tdb`
- `GET /api/tdn`

Migration note:

- category import currently remains helper logic used by mailbox ingestion
- helper code is still duplicated locally for safety and will be reduced later
