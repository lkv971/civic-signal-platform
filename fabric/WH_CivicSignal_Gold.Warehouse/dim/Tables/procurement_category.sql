CREATE TABLE [dim].[procurement_category] (

	[category_id] int NOT NULL, 
	[category] varchar(200) NOT NULL, 
	[sort_order] int NOT NULL, 
	[is_active] bit NOT NULL, 
	[gold_refreshed_at_utc] datetime2(6) NOT NULL
);