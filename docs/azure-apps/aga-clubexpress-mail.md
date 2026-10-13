# aga-clubexpress-mail

Gmail OAuth polling, ClubExpress CSV/email parsers, staged event processing, archive blobs and MySQL reference sync. MySQL is an external reference contract; its schema is not the Azure SQL export and requires separate migration assessment.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-clubexpress-mail/](../../azure-baseline/aga-clubexpress-mail/). Development: [clubexpress-mail-app/](../../clubexpress-mail-app/).
- Host: `aga-clubexpress-mail.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:11:00.753667+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agaclubexpressmail-8387802/released-package.zip`. SHA-256 `ee22678a3bdababf45b81fbd71dc9f450ba98f891aa3b4387ca3449340bf881c`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-clubexpress-mail.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [clubexpress_csv_parsers.py](../../azure-baseline/aga-clubexpress-mail/clubexpress_csv_parsers.py) | [clubexpress-mail-app/clubexpress_csv_parsers.py](../../clubexpress-mail-app/clubexpress_csv_parsers.py) |
| [clubexpress_parsers.py](../../azure-baseline/aga-clubexpress-mail/clubexpress_parsers.py) | [clubexpress-mail-app/clubexpress_parsers.py](../../clubexpress-mail-app/clubexpress_parsers.py) |
| [clubexpress_replay.py](../../azure-baseline/aga-clubexpress-mail/clubexpress_replay.py) | [clubexpress-mail-app/clubexpress_replay.py](../../clubexpress-mail-app/clubexpress_replay.py) |
| [clubexpress_staged_processor.py](../../azure-baseline/aga-clubexpress-mail/clubexpress_staged_processor.py) | [clubexpress-mail-app/clubexpress_staged_processor.py](../../clubexpress-mail-app/clubexpress_staged_processor.py) |
| [clubexpress_staging.py](../../azure-baseline/aga-clubexpress-mail/clubexpress_staging.py) | [clubexpress-mail-app/clubexpress_staging.py](../../clubexpress-mail-app/clubexpress_staging.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-clubexpress-mail/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [api.sp_lookup_members](../../sql/azure-baseline/api/sp_lookup_members.sql) | literal reference in packaged source | application |
| [api.v_current_ratings](../../sql/azure-baseline/api/v_current_ratings.sql) | catalog dependency or attached trigger | application |
| [api.v_member_lookup](../../sql/azure-baseline/api/v_member_lookup.sql) | catalog dependency or attached trigger | application |
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [integration.clubexpress_email_log](../../sql/azure-baseline/integration/clubexpress_email_log.sql) | catalog dependency or attached trigger | application |
| [integration.journal_article_member_match](../../sql/azure-baseline/integration/journal_article_member_match.sql) | catalog dependency or attached trigger | application |
| [integration.journal_review_member_match](../../sql/azure-baseline/integration/journal_review_member_match.sql) | catalog dependency or attached trigger | application |
| [membership.categories](../../sql/azure-baseline/membership/categories.sql) | catalog dependency or attached trigger | application |
| [membership.chapter_membership_history](../../sql/azure-baseline/membership/chapter_membership_history.sql) | catalog dependency or attached trigger | application |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [membership.clubexpress_parsed_event_attempts](../../sql/azure-baseline/membership/clubexpress_parsed_event_attempts.sql) | literal reference in packaged source | application |
| [membership.clubexpress_parsed_events](../../sql/azure-baseline/membership/clubexpress_parsed_events.sql) | literal reference in packaged source | application |
| [membership.member_categories](../../sql/azure-baseline/membership/member_categories.sql) | catalog dependency or attached trigger | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [membership.membership_events](../../sql/azure-baseline/membership/membership_events.sql) | catalog dependency or attached trigger | application |
| [membership.sp_import_chapters](../../sql/azure-baseline/membership/sp_import_chapters.sql) | literal reference in packaged source | application |
| [membership.sp_import_member_categories](../../sql/azure-baseline/membership/sp_import_member_categories.sql) | literal reference in packaged source | application |
| [membership.sp_import_memchap](../../sql/azure-baseline/membership/sp_import_memchap.sql) | literal reference in packaged source | application |
| [membership.sp_log_clubexpress_email](../../sql/azure-baseline/membership/sp_log_clubexpress_email.sql) | literal reference in packaged source | application |
| [membership.sp_process_journal_news_email](../../sql/azure-baseline/membership/sp_process_journal_news_email.sql) | literal reference in packaged source | application |
| [membership.sp_process_membership_renewal](../../sql/azure-baseline/membership/sp_process_membership_renewal.sql) | literal reference in packaged source | application |
| [membership.sp_process_new_member_email](../../sql/azure-baseline/membership/sp_process_new_member_email.sql) | literal reference in packaged source | application |
| [membership.sp_record_clubexpress_parsed_event](../../sql/azure-baseline/membership/sp_record_clubexpress_parsed_event.sql) | literal reference in packaged source | application |
| [membership.sp_update_clubexpress_parsed_event_status](../../sql/azure-baseline/membership/sp_update_clubexpress_parsed_event_status.sql) | literal reference in packaged source | application |
| [ratings.bayrate_reward_reconciliations](../../sql/azure-baseline/ratings/bayrate_reward_reconciliations.sql) | catalog dependency or attached trigger | application |
| [ratings.dbrepl_dirty_1234103437](../../sql/azure-baseline/ratings/dbrepl_dirty_1234103437.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1250103494](../../sql/azure-baseline/ratings/dbrepl_dirty_1250103494.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1266103551](../../sql/azure-baseline/ratings/dbrepl_dirty_1266103551.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.games](../../sql/azure-baseline/ratings/games.sql) | catalog dependency or attached trigger | application |
| [ratings.ratings](../../sql/azure-baseline/ratings/ratings.sql) | literal reference in packaged source | application |
| [ratings.tournaments](../../sql/azure-baseline/ratings/tournaments.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_daily_snapshot](../../sql/azure-baseline/rewards/chapter_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_eligibility_periods](../../sql/azure-baseline/rewards/chapter_eligibility_periods.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_renewal_notice_results](../../sql/azure-baseline/rewards/chapter_renewal_notice_results.sql) | catalog dependency or attached trigger | application |
| [rewards.lot_allocations](../../sql/azure-baseline/rewards/lot_allocations.sql) | catalog dependency or attached trigger | application |
| [rewards.member_daily_snapshot](../../sql/azure-baseline/rewards/member_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.membership_events](../../sql/azure-baseline/rewards/membership_events.sql) | catalog dependency or attached trigger | application |
| [rewards.point_lots](../../sql/azure-baseline/rewards/point_lots.sql) | catalog dependency or attached trigger | application |
| [rewards.redemption_requests](../../sql/azure-baseline/rewards/redemption_requests.sql) | catalog dependency or attached trigger | application |
| [rewards.reward_runs](../../sql/azure-baseline/rewards/reward_runs.sql) | catalog dependency or attached trigger | application |
| [rewards.sp_create_daily_snapshot](../../sql/azure-baseline/rewards/sp_create_daily_snapshot.sql) | literal reference in packaged source | application |
| [rewards.sp_get_pending_chapter_renewals](../../sql/azure-baseline/rewards/sp_get_pending_chapter_renewals.sql) | literal reference in packaged source | application |
| [rewards.sp_process_chapter_renewal_notices](../../sql/azure-baseline/rewards/sp_process_chapter_renewal_notices.sql) | literal reference in packaged source | application |
| [rewards.sp_process_membership_awards](../../sql/azure-baseline/rewards/sp_process_membership_awards.sql) | literal reference in packaged source | application |
| [rewards.sp_process_point_expirations](../../sql/azure-baseline/rewards/sp_process_point_expirations.sql) | literal reference in packaged source | application |
| [rewards.sp_process_rated_game_awards](../../sql/azure-baseline/rewards/sp_process_rated_game_awards.sql) | literal reference in packaged source | application |
| [rewards.sp_process_tournament_awards](../../sql/azure-baseline/rewards/sp_process_tournament_awards.sql) | literal reference in packaged source | application |
| [rewards.sp_record_chapter_renewal_confirmation](../../sql/azure-baseline/rewards/sp_record_chapter_renewal_confirmation.sql) | literal reference in packaged source | application |
| [rewards.sp_record_membership_event](../../sql/azure-baseline/rewards/sp_record_membership_event.sql) | literal reference in packaged source | application |
| [rewards.transactions](../../sql/azure-baseline/rewards/transactions.sql) | catalog dependency or attached trigger | application |
| [staging.chapters](../../sql/azure-baseline/staging/chapters.sql) | literal reference in packaged source | application |
| [staging.dbrepl_dirty_1202103323](../../sql/azure-baseline/staging/dbrepl_dirty_1202103323.sql) | catalog dependency or attached trigger | tooling/replication |
| [staging.dbrepl_dirty_18099105](../../sql/azure-baseline/staging/dbrepl_dirty_18099105.sql) | catalog dependency or attached trigger | tooling/replication |
| [staging.dbrepl_dirty_887674210](../../sql/azure-baseline/staging/dbrepl_dirty_887674210.sql) | catalog dependency or attached trigger | tooling/replication |
| [staging.member_categories](../../sql/azure-baseline/staging/member_categories.sql) | literal reference in packaged source | application |
| [staging.memchap](../../sql/azure-baseline/staging/memchap.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-clubexpress-mail.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-clubexpress-mail/host.json). [Captured dependency declaration](../../azure-baseline/aga-clubexpress-mail/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-clubexpress-mail/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-clubexpress-mail --output ../packages/aga-clubexpress-mail-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-clubexpress-mail --src ../packages/aga-clubexpress-mail-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `create_rewards_daily_snapshot` | type=timerTrigger; schedule=0 10 5 * * * | True |
| `poll_clubexpress_mailbox` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_rewards_membership_awards` | type=timerTrigger; schedule=0 20 5 * * * | True |
| `process_rewards_point_expirations` | type=timerTrigger; schedule=0 40 5 * * * | True |
| `process_rewards_rated_game_awards` | type=timerTrigger; schedule=0 30 5 * * * | True |
| `process_rewards_tournament_awards` | type=timerTrigger; schedule=0 35 5 * * * | True |
| `process_staged_chapter_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_staged_chapter_renewal_notice_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_staged_journal_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_staged_member_category_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_staged_memchap_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_staged_new_member_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `process_staged_renewal_events` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `send_pending_chapter_renewals_email` | type=timerTrigger; schedule=0 50 5 * * * | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

16 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
