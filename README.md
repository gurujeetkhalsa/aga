# AGA Functions Monorepo

This repository contains the AGA Azure Functions codebase split into focused,
deployable apps.

If you are looking for current production code, start with the standalone app
folders below. Each app folder owns one operational surface and should be
deployed independently.

## Official Standalone Apps

| Repo folder | Azure Function App | Responsibility |
| --- | --- | --- |
| `bayrate-app/` | `aga-bayrate` | BayRate tournament rating workflow: preview, stage, review, replay, and commit. |
| `ratings-explorer-display-app/` | `aga-ratings-explorer-display` | Public Ratings Explorer display, search/detail APIs, SGF viewing, and snapshot refresh. |
| `chapter-rewards-display-app/` | `aga-chapter-rewards-display` | Public read-only Chapter Rewards balances and chapter detail report. |
| `chapter-rewards-admin-app/` | `aga-chapter-rewards-admin` | Authorized Chapter Rewards debit, redemption, notes, and receipt workflows. |
| `chapter-rewards-automation-app/` | `aga-chapter-rewards-automation` | Rewards background timers: snapshots, awards, expirations, and pending-renewal digest. |
| `clubexpress-mail-app/` | `aga-clubexpress-mail` | Gmail polling, ClubExpress mailbox ingestion, message classification, and parser orchestration. |
| `membership-data-app/` | `aga-membership-functions` | Membership/chapter imports, member lookup, TD list publishing, and related data endpoints. |
| `clubexpress-sso-probe-app/` | `aga-clubexpress-sso-probe` | Temporary diagnostic receiver for ClubExpress SSO callback discovery. |

## Clean Separated Source Folders

These folders are clean references for lookup and TD list functionality. They
make the repo easier to inspect by keeping those surfaces away from unrelated
membership import and mailbox code. Production URLs remain on
`aga-membership-functions` unless a future decision explicitly creates separate
Azure apps.

| Repo folder | Production Azure Function App | Responsibility |
| --- | --- | --- |
| `aga-lookup-app/` | `aga-membership-functions` | Public AGA member lookup APIs: `AGALookup` and `lookup-members`. |
| `tdlists-app/` | `aga-membership-functions` | TD list generation and short redirect routes. |

Primary public/operator URLs:

- BayRate: `https://aga-bayrate.azurewebsites.net/api/bayrate`
- Ratings Explorer: `https://aga-ratings-explorer-display.azurewebsites.net/api/ratings-explorer`
- Chapter Rewards display: `https://aga-chapter-rewards-display.azurewebsites.net/api/chapter-rewards`
- Chapter Rewards admin: `https://aga-chapter-rewards-admin.azurewebsites.net/api/chapter-rewards/admin`

## Shared Code And SQL

- `bayrate/` contains the BayRate rating engine, report parser, staging,
  replay, commit, auth, and SQL adapter modules used by `bayrate-app/`.
- `rewards/` contains Chapter Rewards processors, SQL, reporting helpers, and
  tests used by the rewards apps.
- `research/` contains non-production experiments and analysis tools. These are
  intentionally outside the deployed app packages.
- `shared/` contains helpers that are intentionally shared by more than one app.
- `scripts/` contains deployment and operations scripts. Use app-specific
  deploy-prep scripts when present.
- `docs/` contains separation notes, deployment notes, architecture notes, and
  historical session memos.

## Legacy Or Miscellaneous Areas

`ratings-explorer-app/` is the older mixed host retained during the transition.
Do not treat it as the source of truth for new BayRate, rewards, or public
Ratings Explorer work unless you are explicitly maintaining that legacy mixed
deployment.

The standalone BayRate deploy package is intentionally allowlisted by
`scripts/prepare-bayrate-deploy.ps1`; sigma experiments, simulations, chart
renderers, history overlays, member merge tooling, and generated BayRate output
artifacts are not deployed to `aga-bayrate`.

Generated and local-only material should not be treated as production source:

- `_deploy/`
- `data/`
- `bayrate/output/`
- `research/bayrate_sigma/output/`
- `*.results.json`
- local `__pycache__/`, `.python_packages/`, and `.venv/` directories

## Deployment Rule Of Thumb

Deploy from the app folder that matches the Azure Function App you are changing.
If a change touches more than one product surface, split it into app-specific
changes or move truly shared logic into `shared/`, `bayrate/`, or `rewards/`.

The current detailed production map is in `docs/deployment-memo.md`; the
separation decisions are documented in:

- `docs/ratings-explorer-display-separation.md`
- `docs/chapter-rewards-display-separation.md`
- `docs/chapter-rewards-admin-separation.md`
- `docs/clubexpress-email-processing-separation.md`
- `docs/bayrate-separation.md`
- `docs/lookup-tdlists-separation.md`
