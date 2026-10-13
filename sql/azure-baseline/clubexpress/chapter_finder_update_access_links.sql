-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_update_access_links] (
    [Chapter_Finder_Update_Access_Link_ID] bigint IDENTITY(1,1) NOT NULL,
    [Chapter_Finder_Entry_ID] bigint NOT NULL,
    [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Club_ID] DEFAULT ((454497)),
    [Chapter_Finder_ID] int NOT NULL,
    [Token_Hash_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Link_Label] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_By] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Expires_At] datetime2(0) NULL,
    [Is_Active] bit NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Is_Active] DEFAULT ((1)),
    [Last_Used_At] datetime2(0) NULL,
    [Use_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Use_Count] DEFAULT ((0)),
    [Last_Submitted_At] datetime2(0) NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_update_access_links] ADD CONSTRAINT [PK_chapter_finder_update_access_links] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Access_Link_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_access_links] ADD CONSTRAINT [UQ_chapter_finder_update_access_links_Token_Hash] UNIQUE NONCLUSTERED ([Token_Hash_Sha256] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_update_access_links_Entry_Active] ON [clubexpress].[chapter_finder_update_access_links] ([Chapter_Finder_Entry_ID] ASC, [Is_Active] ASC, [Expires_At] ASC) INCLUDE ([Chapter_Finder_ID], [Link_Label], [Last_Submitted_At]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_access_links] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_access_links_Use_Count] CHECK ([Use_Count]>=(0));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_access_links] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_update_access_links_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
