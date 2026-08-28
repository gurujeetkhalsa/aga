<!-- Copyright 2026, American Go Association, All rights reserved -->

# Chapter Rewards Admin App

This is the standalone Function App package for Chapter Rewards administration.

It includes:

- Admin page at `/api/chapter-rewards/admin`.
- Authorized read APIs used by the admin page.
- Manual debit preview and post workflows.
- Bayrate rerun reconciliation review, preview, and confirmed application.
- Redemption detail, notes, receipt upload, receipt removal, and receipt file viewing.

It intentionally excludes:

- Public read-only Chapter Rewards display page.
- BayRate operations.
- Ratings Explorer player/tournament display.
- Chapter directory reports.
- SGF import and other mixed app routes.

Official standalone admin URL:

https://aga-chapter-rewards-admin.azurewebsites.net/api/chapter-rewards/admin

Authorization uses the existing `rewards_redemptions` permission in `ratings.admin_permissions`.

Bayrate reconciliations are applied manually from the admin page. The operator previews the persisted old/new chapter totals, confirms the changes, and applies the difference exactly once per Bayrate run. Positive differences create normal two-year point lots; negative differences consume unexpired lots in expiration order. A zero-difference reconciliation is recorded as reviewed without creating a transaction.
