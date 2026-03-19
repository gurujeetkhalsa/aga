IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ratingsync')
BEGIN
    EXEC(N'CREATE SCHEMA [ratingsync]');
END;
GO

/****** Object:  Table [ratingsync].[games]    Script Date: 3/16/2026 5:41:35 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [ratingsync].[games](
	[Game_ID] [int] NULL,
	[Tournament_Code] [nvarchar](max) NULL,
	[Game_Date] [date] NULL,
	[Round] [nvarchar](max) NULL,
	[Pin_Player_1] [int] NULL,
	[Color_1] [nvarchar](max) NULL,
	[Rank_1] [nvarchar](max) NULL,
	[Pin_Player_2] [int] NULL,
	[Color_2] [nvarchar](max) NULL,
	[Rank_2] [nvarchar](max) NULL,
	[Handicap] [int] NULL,
	[Komi] [int] NULL,
	[Result] [nvarchar](max) NULL,
	[Sgf_Code] [nvarchar](max) NULL,
	[Online] [int] NULL,
	[Exclude] [int] NULL,
	[Rated] [int] NULL,
	[Elab_Date] [date] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

