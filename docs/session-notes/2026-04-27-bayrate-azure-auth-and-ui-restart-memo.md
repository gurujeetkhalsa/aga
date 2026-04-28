# BayRate Azure Auth And UI Restart Memo

Date: 2026-04-27

## Current Branch And Commit

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- Latest pushed commit: `d84564d Add BayRate staging and Azure authorization`
- Production Function App: `aga-ratings-explorer`
- Production BayRate URL: `https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate`

## What We Finished

### BayRate Report Handling

- Multi-report preview now continues after report-level parse failures and surfaces all report warnings/errors together.
- Missing SQL connection is fail-fast for the Function App. The app no longer starts in a mode where membership validation silently skips because SQL is unavailable.
- `?` game results are treated as warnings for scheduled-but-unreported games. Those games are ignored for staging/replay if the operator proceeds.
- Run replay persists staged rating rows into `ratings.bayrate_staged_ratings`.
- Commit preview exists and is read-only. It plans production writes but does not execute them from the UI.

### Azure Deployment

- BayRate code is now deployed to `aga-ratings-explorer`.
- A deployment prep script was added because the Azure publish folder needs the sibling `bayrate/` Python package copied beside `ratings-explorer-app/`.
- Deployment script:
  - `scripts/prepare-ratings-explorer-deploy.ps1`
- Deployment notes were updated in:
  - `docs/deployment-memo.md`
  - `ratings-explorer-app/README.md`

### Authorization

- Azure App Service Authentication is enabled on `aga-ratings-explorer`.
- Platform auth remains `AllowAnonymous` so the public Ratings Explorer routes stay public.
- BayRate routes enforce server-side authorization in Python:
  - page
  - preview
  - stage
  - review
  - replay
  - commit-preview
- BayRate auth requires both:
  - an Easy Auth signed-in user principal
  - an active SQL allowlist row in `ratings.bayrate_admins`
- SQL authorization schema:
  - `bayrate/sql/bayrate_authorization_schema.sql`
- The initial current-user aliases were seeded as active BayRate admins.
- Spoofed client-supplied `X-MS-CLIENT-PRINCIPAL-*` headers were tested and did not bypass login.

### Azure Auth Fixes Applied

- Created/configured the Easy Auth app registration:
  - `aga-ratings-explorer-easyauth`
- Enabled ID token issuance after login initially failed with `unsupported_response_type`.
- Added and consented minimal AAD Graph delegated `User.Read` after login failed with `AADSTS650056`.

## Verification

### Tests

Latest local test run before commit:

```powershell
py -3 -m unittest discover bayrate\tests
```

Result:

```text
Ran 48 tests
OK
```

Also compiled the relevant Python files successfully with `py_compile`.

### Production Smoke Checks

- Public `snapshot-status` returned `200`.
- Public `players-startup` returned `200`.
- Unauthenticated BayRate API requests returned `401`.
- Unauthenticated BayRate page redirected to Microsoft login.
- Production deploy listed all BayRate functions.

### Azure Run Results

- RunID `5` was the first successful Azure authorized staging/replay run.
- RunID `6` was a repeat Azure run using the same inputs.
- RunID `5` and RunID `6` matched exactly:
  - same report hashes
  - same warning types/counts
  - same staged rating row counts
  - same production cascade row counts
  - row-by-row rating and sigma differences were `0.0`
- RunID `4` was the comparable local run. RunID `4` vs RunID `5` had only tiny floating-point differences, now best explained as local Windows/Python vs Azure Linux/Python numeric drift.

## Current Safety Boundaries

- Production `ratings.tournaments`, `ratings.games`, and `ratings.ratings` have not been written by BayRate.
- The UI still exposes only production commit preview, not actual production commit execution.
- Any future production commit path should require:
  - BayRate admin authorization
  - explicit production confirmation
  - stale-run checks
  - audit metadata
  - careful handling of existing SGF-linked game rows

## How To Add Another Operator

1. Invite the person into the Azure/Entra tenant as a guest user.
2. Add their identity to `ratings.bayrate_admins`.
3. Prefer adding both:
   - their normal login email
   - their Entra guest UPN alias
4. Include their Entra object id when available.
5. To revoke access, set `Is_Active = 0`.

The operator does not need direct SQL permissions. The Function App uses its SQL connection and checks the signed-in identity against `ratings.bayrate_admins`.

## Known Caveats

- `ratings.bayrate_runs.Created_By` still records the SQL login (`sqladmin`) rather than the human operator. The real human authorization happens before the write, but future audit columns should capture the Easy Auth principal on each staged action.
- Local dev BayRate access now needs `BAYRATE_DEV_AUTH_EMAIL` if testing locally against the auth gate.
- App Service Authentication v1 is currently in use because it matched the existing Azure CLI surface available during this session.
- There are older unrelated untracked files still in the repo working tree. The pushed commit included only the BayRate/auth/deployment files needed for this work.

## Next Session Agenda: UI Tweaks

Start from the production BayRate page:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate
```

Suggested UI focus:

1. Review the staged workflow after Azure login:
   - report input
   - preview warnings
   - write staged run
   - review decisions
   - replay summary
   - commit preview
2. Improve warning display for:
   - unreported `?` games
   - membership/name/rank mismatches
   - generated tournament codes
3. Make the staged rating before/after view easier to inspect.
4. Consider showing the signed-in operator identity somewhere subtle in the BayRate UI.
5. Consider clearer language around “Preview Production Commit” to reinforce that it is read-only.
6. Decide whether `Rating_Delta` / `Sigma_Delta` should be populated with staged-vs-production cascade deltas to reduce confusion in SQL inspection.

## Useful Commands

Prepare deploy package:

```powershell
.\scripts\prepare-ratings-explorer-deploy.ps1
```

Publish from the generated `_deploy\ratings-explorer-app-bayrate-<timestamp>\` folder:

```powershell
func azure functionapp publish aga-ratings-explorer --python --build remote
```

Run tests:

```powershell
py -3 -m unittest discover bayrate\tests
```
