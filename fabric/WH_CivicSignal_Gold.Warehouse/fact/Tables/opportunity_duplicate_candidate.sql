CREATE TABLE [fact].[opportunity_duplicate_candidate] (

	[duplicate_group_hint] varchar(500) NOT NULL, 
	[opportunity_key] varchar(64) NOT NULL, 
	[source_system] varchar(200) NOT NULL, 
	[opportunity_id] varchar(500) NOT NULL, 
	[title] varchar(2000) NULL, 
	[buyer_name] varchar(1000) NULL, 
	[published_date] date NULL, 
	[potential_duplicate_flag] bit NOT NULL, 
	[silver_run_id] varchar(100) NULL, 
	[silver_processed_at_utc] datetime2(6) NULL, 
	[gold_refreshed_at_utc] datetime2(6) NOT NULL
);