<!-- Copyright 2026, American Go Association, All rights reserved -->

# Chapter Rewards Display App

This is the standalone Function App package for the public Chapter Rewards display surface.

It includes:

- Public Chapter Rewards page.
- Read-only chapter balance summary API.
- Read-only per-chapter ledger, lot, breakdown, and posted-redemption detail API.

It intentionally excludes:

- Rewards admin authorization.
- Manual debit preview/post workflows.
- Receipt upload, deletion, notes, and receipt file serving.
- BayRate, Ratings Explorer display, chapter directory, SGF import, and other mixed app routes.

Official standalone display URL:

https://aga-chapter-rewards-display.azurewebsites.net/api/chapter-rewards
