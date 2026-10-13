-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[chapter_renewal_notice_results] (
    [NoticeID] bigint IDENTITY(1,1) NOT NULL,
    [Message_ID] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Row_Number] int NOT NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Notice_Date] date NOT NULL,
    [Received_At] datetime2(0) NOT NULL,
    [Points_Required] int NOT NULL,
    [Available_Points] int NOT NULL,
    [Decision] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [RedemptionID] bigint NULL,
    [TransactionID] bigint NULL,
    [RunID] int NULL,
    [Source_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_renewal_notice_results_Created_At] DEFAULT (sysutcdatetime()),
    [ClubExpress_Renewal_Message_ID] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ClubExpress_Renewed_At] datetime2(0) NULL,
    [ClubExpress_Renewal_Recorded_At] datetime2(0) NULL,
    [ClubExpress_Renewal_Source_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] ADD CONSTRAINT [PK_chapter_renewal_notice_results] PRIMARY KEY CLUSTERED ([NoticeID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] ADD CONSTRAINT [UQ_chapter_renewal_notice_results_Message_Chapter] UNIQUE NONCLUSTERED ([Message_ID] ASC, [ChapterID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_renewal_notice_results_Decision] ON [rewards].[chapter_renewal_notice_results] ([Decision] ASC, [Notice_Date] ASC) INCLUDE ([ChapterID], [Chapter_Code], [Chapter_Name], [Points_Required], [Available_Points]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_renewal_notice_results_Pending] ON [rewards].[chapter_renewal_notice_results] ([ClubExpress_Renewal_Message_ID] ASC, [Decision] ASC, [Notice_Date] ASC) INCLUDE ([ChapterID], [Chapter_Code], [Chapter_Name], [Points_Required], [TransactionID]) WHERE ([ClubExpress_Renewal_Message_ID] IS NULL AND [TransactionID] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] WITH CHECK ADD CONSTRAINT [CK_chapter_renewal_notice_results_Points_Required] CHECK ([Points_Required]>(0));
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] WITH CHECK ADD CONSTRAINT [CK_chapter_renewal_notice_results_Available_Points] CHECK ([Available_Points]>=(0));
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] WITH CHECK ADD CONSTRAINT [CK_chapter_renewal_notice_results_Decision] CHECK ([Decision]=N'chapter_not_found' OR [Decision]=N'insufficient_points' OR [Decision]=N'already_posted' OR [Decision]=N'posted');
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] WITH CHECK ADD CONSTRAINT [FK_chapter_renewal_notice_results_RedemptionID] FOREIGN KEY ([RedemptionID]) REFERENCES [rewards].[redemption_requests] ([RedemptionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] WITH CHECK ADD CONSTRAINT [FK_chapter_renewal_notice_results_TransactionID] FOREIGN KEY ([TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [rewards].[chapter_renewal_notice_results] WITH CHECK ADD CONSTRAINT [FK_chapter_renewal_notice_results_RunID] FOREIGN KEY ([RunID]) REFERENCES [rewards].[reward_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
