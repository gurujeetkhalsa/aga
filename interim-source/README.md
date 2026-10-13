# Preserved local development source

These folders are source snapshots, **not Azure deployment baselines**. See
[manifest.json](manifest.json) for original branch/commit and per-file SHA-256.
The manifest also records credential redactions: three hard-coded Azure Function
URL keys in `original-functions/function_app.py` are secure-supply placeholders
in the published snapshot. Its original source hash is retained for comparison.
The original worktrees, staged state and untracked files were left unchanged.
No branch switch, reset, stash or forced push was performed there.

`playground-current` is the main local AGA development checkout;
`bayrate-reconciliation` is its BayRate reconciliation worktree;
`original-functions` and `sgf-phase1` are earlier separate AGA checkouts.
Existing remote feature branches remain in GitHub. The original GitHub default
branch and history are retained as `master`; the new baseline is on `main`.

Source, necessary artwork, tests, build/deployment scripts and SQL migrations were
allowlisted. Production CSV/JSON exports, mail/news samples, runtime output,
environments, dependencies, local credentials and temporary/rollback bundles are
intentionally not published. This is not a full backup of private local files.
Historical operational/session notes were also excluded to avoid republishing
production details; changed design/deployment/migration documentation is retained.
Source differences are indexed in [the app details](../docs/azure-source-map.md).
Do not assume a later local timestamp means intended production behavior.

For future VM work use the mapped development folders and an explicitly established
VM implementation directory, not these historical snapshots. Translate intended
changes through the [three-way reconciliation procedure](../docs/reconcile-azure-changes.md).
