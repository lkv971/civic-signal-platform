CREATE TABLE [dim].[date] (

	[date_key] int NOT NULL, 
	[date_value] date NOT NULL, 
	[year_number] int NOT NULL, 
	[quarter_number] int NOT NULL, 
	[quarter_label] varchar(10) NOT NULL, 
	[month_number] int NOT NULL, 
	[month_name] varchar(20) NOT NULL, 
	[year_month] varchar(7) NOT NULL, 
	[month_start_date] date NOT NULL, 
	[month_end_date] date NOT NULL, 
	[iso_week_number] int NOT NULL
);