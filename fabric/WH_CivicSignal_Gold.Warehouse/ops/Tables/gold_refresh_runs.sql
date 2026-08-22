CREATE TABLE [ops].[gold_refresh_runs] (

	[gold_run_id] varchar(100) NOT NULL, 
	[started_at_utc] datetime2(6) NOT NULL, 
	[completed_at_utc] datetime2(6) NOT NULL, 
	[source_rows] bigint NOT NULL, 
	[buyer_rows] bigint NOT NULL, 
	[category_rows] bigint NOT NULL, 
	[opportunity_rows] bigint NOT NULL, 
	[duplicate_candidate_rows] bigint NOT NULL, 
	[orphan_buyer_rows] bigint NOT NULL, 
	[status] varchar(50) NOT NULL
);