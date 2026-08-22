CREATE TABLE [dim].[buyer] (

	[buyer_key] varchar(64) NOT NULL, 
	[buyer_id] varchar(500) NOT NULL, 
	[buyer_name] varchar(1000) NULL, 
	[buyer_type] varchar(200) NULL, 
	[country] varchar(200) NULL, 
	[country_code] varchar(20) NULL, 
	[region] varchar(200) NULL, 
	[website] varchar(2000) NULL, 
	[source_system] varchar(200) NOT NULL, 
	[first_seen_at_utc] datetime2(6) NULL, 
	[last_seen_at_utc] datetime2(6) NULL, 
	[silver_run_id] varchar(100) NULL, 
	[silver_processed_at_utc] datetime2(6) NULL, 
	[gold_refreshed_at_utc] datetime2(6) NOT NULL
);