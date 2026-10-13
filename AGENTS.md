# AGA source and migration work

Read [docs/azure-source-map.md](docs/azure-source-map.md) first for every Azure app,
its exact deployed copy, development folder, shared imports, live SQL definitions,
configuration contract and packaging/deployment instructions.

`azure-baseline/<Azure-app-name>/` contains captured deployments. Keep deployments
separate even when filenames match. Do not edit these into a new implementation.
Use the mapped development folders and a separately established `vm-stack/` for
future VM/PostgreSQL work. `interim-source/` preserves local work, not proof of deployment.
Live schema is in `sql/azure-baseline/`; read both SQL and metadata companions.

Follow [docs/reconcile-azure-changes.md](docs/reconcile-azure-changes.md) for fresh
read-only capture and three-way comparison with PREMIGRATION and the migrated stack.
Never overwrite migrated code with a whole Azure package. Never move PREMIGRATION.
No production changes, side-effecting route calls, mail, Zoom creation, queue
consumption or SQL mutation are authorized by a source-capture request.

Use the explicitly selected AGA subscription for AGA work:
`4dcce4a0-c902-4012-91fe-6dbbddfd6fb7`, tenant
`5f1a8692-caac-4040-9a1b-858e98f03e4b`. Personal work uses the user's separately
verified personal subscription; AGA credentials/resources never authorize that use.
Do not commit secrets, signed URLs, user data, installed dependencies or runtime caches.
Preserve dirty worktrees and all history. Validate packages statically or in an
isolated environment with production access and outbound integrations disabled.
