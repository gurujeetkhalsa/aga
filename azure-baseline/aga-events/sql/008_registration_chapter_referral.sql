-- Optional chapter attribution. Existing signups remain unattributed.
SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF COL_LENGTH('events.registrations', 'referral_chapter_id') IS NULL
    ALTER TABLE events.registrations ADD referral_chapter_id int NULL;
COMMIT TRANSACTION;
