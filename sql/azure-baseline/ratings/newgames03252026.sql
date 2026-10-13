-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[newgames03252026] (
    [Game_ID] int NOT NULL,
    [Tournament_Code] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Game_Date] date NOT NULL,
    [Round] tinyint NOT NULL,
    [Pin_Player_1] smallint NOT NULL,
    [Color_1] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Rank_1] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Pin_Player_2] smallint NOT NULL,
    [Color_2] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Rank_2] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Handicap] tinyint NOT NULL,
    [Komi] tinyint NOT NULL,
    [Result] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Sgf_Code] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Online] tinyint NOT NULL,
    [Exclude] tinyint NOT NULL,
    [Rated] tinyint NOT NULL,
    [Elab_Date] date NOT NULL
 );
GO
