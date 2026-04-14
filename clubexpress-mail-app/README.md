# ClubExpress Mail App

This app contains:

- Gmail mailbox polling
- ClubExpress message classification
- attachment extraction and archiving
- journal parsing
- NAOL review parsing
- mailbox-driven import orchestration

Current migration source:

- `C:\Users\guruj\aga-functions\function_app.py`

Current trigger surface:

- timer: `poll_clubexpress_mailbox`

Migration note:

- this is a first-pass split from the legacy monolith
- helper code is still duplicated locally for safety and will be reduced later
