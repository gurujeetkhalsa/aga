<!-- Copyright 2026, American Go Association, All rights reserved -->

# Chapter Finder Update Portal

This Azure Function app provides:

- a chapter-specific, prefilled update form protected by a high-entropy link;
- an Azure Easy Auth administrator review queue;
- approval and rejection endpoints backed by the Chapter Finder SQL workflow;
- an approved push queue for the authorized Chrome ClubExpress step.

The push step is browser-assisted and uses the exact, guarded control map in
`../chapter-finder-sync/CLUBEXPRESS_PUSH.md`. It does not store or replay a
ClubExpress admin session.

Raw chapter access tokens are never stored. SQL stores only SHA-256 token
hashes. ClubExpress credentials, cookies, passwords, and MFA data are never
stored by the app.

Required app settings:

- `SQL_CONNECTION_STRING`
- `BAYRATE_TRUST_EASY_AUTH=true`
- `CHAPTER_UPDATE_PUBLIC_BASE_URL`

Review authorization uses the `chapter_finder_updates` permission in
`ratings.admin_permissions`; `admin_all` also grants access.

Production endpoints:

- Form: `https://aga-chapter-finder-update.azurewebsites.net/api/chapter-finder/update`
- Review: `https://aga-chapter-finder-update.azurewebsites.net/api/chapter-finder/review`

Deploy from this folder:

```powershell
func azure functionapp publish aga-chapter-finder-update --python --build remote
```

Apply these schemas before deployment:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py apply-schema
py -3 aga\chapter-finder-sync\chapter_finder_update_portal.py apply-schema
```

Generate a chapter link:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_portal.py create-link `
  --clubexpress-chapter-id 5518 `
  --base-url https://aga-chapter-finder-update.azurewebsites.net/api/chapter-finder/update
```
