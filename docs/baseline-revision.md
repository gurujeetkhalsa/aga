# Baseline publication

Repository: https://github.com/gurujeetkhalsa/aga (public, existing visibility).

| Repository | Annotated tag | Exact baseline commit | Publication |
|---|---|---|---|
| [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga) | [PREMIGRATION](https://github.com/gurujeetkhalsa/aga/tree/PREMIGRATION) | [`d4cbecdfafdd7b71f97b92e804fbdc6f6753ebaf`](https://github.com/gurujeetkhalsa/aga/commit/d4cbecdfafdd7b71f97b92e804fbdc6f6753ebaf) | Published on `migration`, fast-forwarded into `main`; no pending baseline PR |

`main` and `migration` include a documentation-only follow-up recording this exact
revision. PREMIGRATION continues to identify the original source/schema commit;
it was not moved. There are no additional repositories in this baseline.
The original `master` and feature-branch histories remain available.

Source recovered: 16/16 Function Apps. Live SQL: 187 application/tooling objects,
with full table/index/constraint/permission metadata and no unavailable module
definitions. Original `master` history and remote feature branches are retained.
No additional repository is needed. Azure production is unchanged.

Known verification difference: the captured workshop package contains two email
formatting tests with three failing assertions (42 tests executed offline). The
deployed HTML reminder behavior differs from the bundled tests' older expectations.
The exact source and tests are retained. See [verification notes](verification-notes.md).
