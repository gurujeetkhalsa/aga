# AGA Functions Monorepo

This repository is the clean rebuild of the AGA Azure Functions codebase.

## App layout

- `ratings-explorer-app/`
  Ratings Explorer UI, APIs, SGF support, and snapshot jobs.
- `clubexpress-mail-app/`
  Gmail polling, ClubExpress mailbox ingestion, attachment processing, and message archiving.
- `membership-data-app/`
  Membership/chapter imports, member lookup, and related SQL-backed data endpoints.
- `tdlists-app/`
  TD list publishing endpoints and related rendering helpers.
- `shared/`
  Shared helpers used by more than one app.
- `docs/`
  Architecture notes, migration notes, and setup documentation.

## Current migration approach

We are moving from a mixed local codebase into a clean monorepo in stages:

1. Create the repo structure and documentation.
2. Move `ratings-explorer-app` in first because it is already mostly isolated.
3. Split mailbox ingestion into `clubexpress-mail-app`.
4. Split TD list endpoints into `tdlists-app`.
5. Split membership import and lookup into `membership-data-app`.
6. Pull only true cross-app helpers into `shared/`.

## Source of truth during migration

Until each app is copied into this repo, the current live source files remain in:

- `C:\Users\guruj\aga-functions\function_app.py`
- `C:\Users\guruj\aga-functions\ratings-explorer-app\`

See `docs/migration-plan.md` for the function-by-function mapping.

