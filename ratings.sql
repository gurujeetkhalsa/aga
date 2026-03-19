IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ratingsync')
BEGIN
    EXEC(N'CREATE SCHEMA [ratingsync]');
END;
GO

/****** Object:  Table [ratingsync].[ratings]    Script Date: 3/16/2026 5:47:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [ratingsync].[ratings](
	[Pin_Player] [int] NULL,
	[Rating] [float] NULL,
	[Sigma] [float] NULL,
	[Elab_Date] [date] NULL,
	[Tournament_Code] [nvarchar](max) NULL,
	[id] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

