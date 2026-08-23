CREATE TABLE [dim].[source] (

	[source_system] varchar(200) NOT NULL, 
	[source_display_name] varchar(500) NULL, 
	[source_type] varchar(100) NULL, 
	[enabled_flag] int NULL, 
	[supported_entity_types] varchar(500) NULL, 
	[schema_versions] varchar(200) NULL, 
	[is_synthetic] bit NOT NULL, 
	[silver_run_id] varchar(100) NULL, 
	[silver_processed_at_utc] datetime2(6) NULL, 
	[gold_refreshed_at_utc] datetime2(6) NOT NULL
);