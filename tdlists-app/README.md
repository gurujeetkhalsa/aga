# TD Lists App

This app contains:

- TD list generation endpoints
- short redirect endpoints
- TD list query and rendering helpers

Current migration source:

- `C:\Users\guruj\aga-functions\function_app.py`

Current trigger surface:

- `GET /api/GenerateTDListA`
- `GET /api/GenerateTDListB`
- `GET /api/GenerateTDListN`
- `GET /api/tda`
- `GET /api/tdb`
- `GET /api/tdn`

Migration note:

- helper code is still duplicated locally for safety and will be reduced later
