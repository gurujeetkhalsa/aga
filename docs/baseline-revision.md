# Baseline publication

Repository: https://github.com/gurujeetkhalsa/aga (public, existing visibility).

The annotated `PREMIGRATION` tag identifies this source/schema baseline. Its exact
commit will be recorded in a publication-only follow-up commit after the immutable
tag is created; a commit cannot contain its own hash. No source recapture or tag
movement is implied by that publication record.

Source recovered: 16/16 Function Apps. Live SQL: 187 application/tooling objects,
with full table/index/constraint/permission metadata and no unavailable module
definitions. Original `master` history and remote feature branches are retained.
No additional repository is needed. Azure production is unchanged.

Known verification difference: the captured workshop package contains two email
formatting tests with three failing assertions (42 tests executed offline). The
deployed HTML reminder behavior differs from the bundled tests' older expectations.
The exact source and tests are retained. See [verification notes](verification-notes.md).
