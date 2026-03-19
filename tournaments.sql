IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ratingsync')
BEGIN
    EXEC(N'CREATE SCHEMA [ratingsync]');
END;
GO

/****** Object:  Table [ratingsync].[tournaments]    Script Date: 3/16/2026 5:45:23 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [ratingsync].[tournaments](
	[Tournament_Code] [nvarchar](max) NULL,
	[Tournament_Descr] [nvarchar](max) NULL,
	[Tournament_Date] [date] NULL,
	[City] [nvarchar](max) NULL,
	[State_Code] [nvarchar](max) NULL,
	[Country_Code] [nvarchar](max) NULL,
	[Rounds] [int] NULL,
	[Total_Players] [int] NULL,
	[Wallist] [nvarchar](max) NULL,
	[Elab_Date] [date] NULL,
	[status] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

