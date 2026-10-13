-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[newtournaments03252026] (
    [Tournament_Code] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Tournament_Descr] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Tournament_Date] date NOT NULL,
    [City] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [State_Code] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Country_Code] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Rounds] tinyint NOT NULL,
    [Total_Players] tinyint NOT NULL,
    [Wallist] nvarchar(1050) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Elab_Date] date NOT NULL,
    [status] tinyint NOT NULL
 );
GO
