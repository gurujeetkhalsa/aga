<!-- Copyright 2026, American Go Association, All rights reserved -->

# Ratings Explorer Display App

This is the separated Function App candidate for the public Ratings Explorer display surface.

Official standalone display URL:

https://aga-ratings-explorer-display.azurewebsites.net/api/ratings-explorer

It includes:

- Ratings Explorer desktop and mobile pages.
- Player search, startup players, player detail, and player context APIs.
- Tournament search and tournament detail APIs.
- Linked-game SGF viewing and static viewer assets.
- Player history SVG rendering.
- Ratings Explorer snapshot status, warm, refresh, and timer jobs.

It intentionally excludes:

- BayRate staging, preview, replay, commit, and member-merge workflows.
- Rewards public/admin reports and reward posting tools.
- Chapter directory reports.
- Go services prototypes.
- Leago/SGF import and upload workflows.

The mixed production app remains `ratings-explorer-app` for BayRate, rewards, chapter reports, and import/admin tools. Keep changes here display-only; if a new route needs write access, admin auth, or tournament processing, it belongs in a separate package instead.
