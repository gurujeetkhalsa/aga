<!-- Copyright 2026, American Go Association, All rights reserved -->

# Chapter Rewards Admin App

This is the standalone Function App package for Chapter Rewards administration.

It includes:

- Admin page at `/api/chapter-rewards/admin`.
- Authorized read APIs used by the admin page.
- Manual debit preview and post workflows.
- Chapter-to-chapter point transfer preview and post workflows.
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

Chapter transfers use `rewards.sp_post_chapter_transfer`. The source chapter is debited
FIFO from unexpired point lots, and the destination receives matching lots that retain
the original earned and expiration dates. An external transfer ID makes retries
idempotent, and the debit, credits, lot allocations, and transfer audit row post in one
database transaction.

Before deploying the app version that exposes the transfer routes, apply
`rewards/sql/transfer_processing.sql` to the rewards database so the transfer
audit table and stored procedure exist.
