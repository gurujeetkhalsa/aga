<!-- Copyright 2026, American Go Association, All rights reserved -->

# Live Stored Procedure Snapshot

This folder contains scripted definitions for every non-system stored procedure found in the AGA Azure SQL database during the 2026-06-28T16:27:42Z audit.

These files are a complete GitHub snapshot for inspection and recovery. App-specific SQL files under folders like `rewards/sql/`, `bayrate/sql/`, and `membership-data-app/sql/` remain the hand-maintained migration sources for their subsystems.

The export normalizes procedure headers to `CREATE OR ALTER PROCEDURE` and includes the live ANSI NULLS and quoted identifier settings. It does not contain connection strings or credentials.

## Coverage

- Live user stored procedures exported: 70
- Procedure definitions already found in tracked app-specific SQL files before this snapshot: 22
- Live procedures missing from app-specific tracked SQL before this snapshot: 48
- Tracked procedure definitions not found live: 0

## Exported Procedures

- `[api].[sp_lookup_members]`
- `[competition].[sp_mark_obvious_duplicate_review]`
- `[competition].[sp_refresh_clean_competition]`
- `[competition].[sp_refresh_duplicate_review]`
- `[competition].[sp_refresh_game_duplicate_review]`
- `[competition].[sp_refresh_tournament_duplicate_review]`
- `[dbo].[sp_alterdiagram]`
- `[dbo].[sp_creatediagram]`
- `[dbo].[sp_dropdiagram]`
- `[dbo].[sp_helpdiagramdefinition]`
- `[dbo].[sp_helpdiagrams]`
- `[dbo].[sp_renamediagram]`
- `[dbo].[sp_upgraddiagrams]`
- `[membership].[sp_find_invalid_youth_members]`
- `[membership].[sp_import_chapters]`
- `[membership].[sp_import_member_categories]`
- `[membership].[sp_import_memchap]`
- `[membership].[sp_log_clubexpress_email]`
- `[membership].[sp_process_journal_news_email]`
- `[membership].[sp_process_membership_renewal]`
- `[membership].[sp_process_new_member_email]`
- `[membership].[sp_record_clubexpress_parsed_event]`
- `[membership].[sp_update_clubexpress_parsed_event_status]`
- `[ratings].[RestoreGamesSgfState]`
- `[ratings].[SaveGamesSgfState]`
- `[ratings].[sp_bayrate_complete_run]`
- `[ratings].[sp_bayrate_fail_run]`
- `[ratings].[sp_bayrate_get_checkpoint_before_event]`
- `[ratings].[sp_bayrate_get_cutover_control]`
- `[ratings].[sp_bayrate_get_replay_boundary]`
- `[ratings].[sp_bayrate_get_snapshot_seed_before_event]`
- `[ratings].[sp_bayrate_mark_dirty_from_event]`
- `[ratings].[sp_bayrate_mark_dirty_from_tournament]`
- `[ratings].[sp_bayrate_mark_event_computed]`
- `[ratings].[sp_bayrate_purge_from_event]`
- `[ratings].[sp_bayrate_refresh_event_index]`
- `[ratings].[sp_bayrate_set_cutover_control]`
- `[ratings].[sp_bayrate_start_run]`
- `[ratings].[sp_grant_admin_permission]`
- `[ratings].[sp_legacy_bayrate_complete_run]`
- `[ratings].[sp_legacy_bayrate_fail_run]`
- `[ratings].[sp_legacy_bayrate_get_checkpoint_before_event]`
- `[ratings].[sp_legacy_bayrate_get_replay_boundary]`
- `[ratings].[sp_legacy_bayrate_mark_dirty_from_event]`
- `[ratings].[sp_legacy_bayrate_mark_dirty_from_tournament]`
- `[ratings].[sp_legacy_bayrate_mark_event_computed]`
- `[ratings].[sp_legacy_bayrate_purge_from_event]`
- `[ratings].[sp_legacy_bayrate_refresh_event_index]`
- `[ratings].[sp_legacy_bayrate_start_run]`
- `[ratings].[sp_revoke_admin_permission]`
- `[rewards].[sp_add_redemption_receipt]`
- `[rewards].[sp_backfill_chapter_renewal_confirmations]`
- `[rewards].[sp_create_daily_snapshot]`
- `[rewards].[sp_delete_redemption_receipt]`
- `[rewards].[sp_get_pending_chapter_renewals]`
- `[rewards].[sp_import_legacy_gap_membership_awards]`
- `[rewards].[sp_import_legacy_gap_tournament_awards]`
- `[rewards].[sp_import_legacy_redemptions]`
- `[rewards].[sp_import_legacy_redemptions_with_adjustments]`
- `[rewards].[sp_import_opening_balances]`
- `[rewards].[sp_post_manual_redemption]`
- `[rewards].[sp_process_chapter_renewal_notices]`
- `[rewards].[sp_process_legacy_gap_rated_game_awards]`
- `[rewards].[sp_process_membership_awards]`
- `[rewards].[sp_process_point_expirations]`
- `[rewards].[sp_process_rated_game_awards]`
- `[rewards].[sp_process_tournament_awards]`
- `[rewards].[sp_record_chapter_renewal_confirmation]`
- `[rewards].[sp_record_membership_event]`
- `[rewards].[sp_update_redemption_notes]`
