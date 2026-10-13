-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[chapter_eligibility_periods] (
    [Chapter_Eligibility_Period_ID] int IDENTITY(1,1) NOT NULL,
    [ChapterID] int NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Effective_Start_Date] date NOT NULL,
    [Effective_End_Date] date NULL,
    [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_eligibility_periods_Is_Current] DEFAULT ((1)),
    [Source] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_eligibility_periods_Source] DEFAULT (N'manual'),
    [Notes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_eligibility_periods_Created_At] DEFAULT (sysutcdatetime()),
    [Created_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT [DF_chapter_eligibility_periods_Created_By] DEFAULT (suser_sname())
 );
GO
ALTER TABLE [rewards].[chapter_eligibility_periods] ADD CONSTRAINT [PK_chapter_eligibility_periods] PRIMARY KEY CLUSTERED ([Chapter_Eligibility_Period_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_eligibility_periods_Chapter_Date] ON [rewards].[chapter_eligibility_periods] ([Chapter_Code] ASC, [Effective_Start_Date] ASC, [Effective_End_Date] ASC) INCLUDE ([ChapterID], [Is_Current]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[chapter_eligibility_periods] WITH CHECK ADD CONSTRAINT [CK_chapter_eligibility_periods_Date_Range] CHECK ([Effective_End_Date] IS NULL OR [Effective_End_Date]>=[Effective_Start_Date]);
GO
