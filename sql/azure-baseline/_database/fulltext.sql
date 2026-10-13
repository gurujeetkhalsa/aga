CREATE FULLTEXT CATALOG [clubexpress_news_archive_search] WITH ACCENT_SENSITIVITY = ON AS DEFAULT AUTHORIZATION [dbo];
GO
CREATE FULLTEXT INDEX ON [clubexpress].[news_article_snapshots] ([Headline] LANGUAGE 1033, [Summary] LANGUAGE 1033, [Search_Text] LANGUAGE 1033) KEY INDEX [PK_news_article_snapshots] ON [clubexpress_news_archive_search] WITH (CHANGE_TRACKING = AUTO, STOPLIST = SYSTEM);
GO
