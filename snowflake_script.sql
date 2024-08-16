-- select database
use project;

-- creating file format 
create FILE FORMAT parquet_format TYPE = parquet;

-- create storage integration for gcs
create or replace storage integration news_api_gcs_int
type = external_stage
storage_provider = gcs
enabled = True
storage_allowed_locations = ('gcs://snowflake-projects/news_api_project/parquet_files/');

-- checking service account name 
desc integration news_api_gcs_int;

-- create stage 
create or replace stage news_raw_data_stage
url = 'gcs://snowflake-projects/news_api_project/parquet_files/'
storage_integration = news_api_gcs_int
file_format = (type = 'parquet');

show stages;

-- see the loaded data

select * FROM news_api_data;