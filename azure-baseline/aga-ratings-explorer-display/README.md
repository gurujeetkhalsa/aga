# Ratings Explorer Display App

This is the separated Function App candidate for the public Ratings Explorer display surface.

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

The current production app remains `ratings-explorer-app` until this candidate has been tested and given its own Azure Function App target. Keep changes here display-only; if a new route needs write access, admin auth, or tournament processing, it belongs in a separate package instead.
