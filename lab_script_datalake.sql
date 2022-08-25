-- This SQL file is for the Hands On Lab Guide for the 30-day free Snowflake trial account (Enterprise edition)
-- The numbers below correspond to the sections of the Lab Guide in which SQL is to be run in a Snowflake worksheet

/* *********************************************************************************** */
/* *** MODULE 1  ********************************************************************* */
/* *********************************************************************************** */

-- 1.2.1 Create a virtual warehouse cluster

use role SYSADMIN;
create or replace warehouse LOAD_WH with
  warehouse_size = 'xlarge'
  auto_suspend = 300
  initially_suspended = true;

use warehouse LOAD_WH;

-- 1.2.2 Create the new empty CITIBIKE_LAB database

create or replace database CITIBIKE_LAB;
create or replace schema DEMO;
create or replace schema UTILS;

-- 1.2.3  Create an API integration to support creating an external function call to a REST API

use schema UTILS;
use role accountadmin;

-- Create an API Integration object
create or replace api integration fetch_http_data
  api_provider = aws_api_gateway
  api_aws_role_arn = 'arn:aws:iam::148887191972:role/ExecuteLambdaFunction'
  enabled = true
  api_allowed_prefixes = ('https://dr14z5kz5d.execute-api.us-east-1.amazonaws.com/prod/fetchhttpdata');

grant usage on integration fetch_http_data to role sysadmin;

-- 1.2.4
-- Now create the external function that uses the API integration object
use role sysadmin;

-- create the function
create or replace external function utils.fetch_http_data(v varchar)
    returns variant
    api_integration = fetch_http_data
    as 'https://dr14z5kz5d.execute-api.us-east-1.amazonaws.com/prod/fetchhttpdata';


-- 1.2.5 Create a few reference tables and populate them with data.

use schema DEMO;
create or replace table GBFS_JSON (
  data	varchar,
  url	varchar,
  payload	variant,
  row_inserted	timestamp_ltz);

-- Populate it with raw JSON data through the External Function call
insert into GBFS_JSON
select
  $1 data,
  $2 url,
  utils.fetch_http_data( url ) payload,
  current_timestamp() row_inserted
from
  (values
    ('stations', 'https://gbfs.citibikenyc.com/gbfs/en/station_information.json'),
    ('regions', 'https://gbfs.citibikenyc.com/gbfs/en/system_regions.json'));

-- Now refine that raw JSON data by extracting out the STATIONS nodes
create or replace table STATION_JSON as
with s as (
  select payload, row_inserted
    from gbfs_json
   where data = 'stations'
     and row_inserted = (select max(row_inserted) from gbfs_json)
  )
select
  value station_v,
  payload:response.last_updated::timestamp last_updated,
  row_inserted
from s,
  lateral flatten (input => payload:response.data.stations) ;

-- extract the individual region records
create or replace table REGION_JSON as
with r as (
  select payload, row_inserted
    from gbfs_json
   where data = 'regions'
     and row_inserted = (select max(row_inserted) from gbfs_json)
  )
select
  value region_v,
  payload:response.last_updated::timestamp last_updated,
  row_inserted
from r,
  lateral flatten (input => payload:response.data.regions);

-- Lastly, create a view that "flattens" the JSON into a standard table structure
create or replace view STATIONS_VW as
with s as (
  select * from station_json
   where row_inserted = (select max(row_inserted) from station_json)
   ),
     r as (
  select * from region_json
   where row_inserted = (select max(row_inserted) from region_json))
select
  station_v:station_id::number   station_id,
  station_v:name::string         station_name,
  station_v:lat::float           station_lat,
  station_v:lon::float           station_lon,
  station_v:station_type::string station_type,
  station_v:capacity::number     station_capacity,
  station_v:rental_methods       rental_methods,
  region_v:name::string          region_name
from s
  left outer join r
    on station_v:region_id::integer = region_v:region_id::integer ;


/* *********************************************************************************** */
/* *** MODULE 2  ********************************************************************* */
/* *********************************************************************************** */

-- 2.1.1  Set context

use schema demo;
use role SYSADMIN;

create or replace stage CITIBIKE_STAGE
  url = 's3://sfquickstarts/VHOL Snowflake for Data Lake/'
  file_format=(type=parquet);

-- 2.1.3 Let’s see what data is available:
-- Show the list of files in the external stage

list @citibike_stage/Data/2015;

-- Let’s take a peek inside the files themselves
select $1 from @citibike_stage/Data/2015 limit 100;

-- 2.1.4 Create a basic External Table on the Parquet files
--       in the bucket

create or replace file format citibike_parquet_ff
  type = parquet;

create or replace external table TRIPS_BASIC_XT
  location = @citibike_stage/Data
  auto_refresh = false
  file_format=(format_name=citibike_parquet_ff);

-- Take a look at the raw data, and use the metadata$filename
-- pseudocolumn to see which row lives in which source file.

  select metadata$filename, value
    from TRIPS_BASIC_XT
    LIMIT 100;

-- 2.1.5
-- Create a new external table on the same set of Parquet
-- files, but this time we’ll define each column
-- separately, and partition the files on the date
-- portion of their folder names. We’ll use Snowflake’s
-- infer_schema and GENERATE_COLUMN_DESCRIPTION functionality
-- to help us create the table definition

select *
from table(
  infer_schema(
    location=>'@citibike_stage/Data'
    , file_format=>'citibike_parquet_ff'
    )
  );

-- Use the INFER_SCHEMA and GENERATE_COLUMN_DESCRIPTION
-- functions to build the CREATE EXTERNAL TABLE statement, which
-- we will then customize.

SELECT
  $$ CREATE OR REPLACE EXTERNAL TABLE FOO ($$ || (
      SELECT
      GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*))
        , 'EXTERNAL_TABLE') ||
        $$) LOCATION = @citibike_stage/Data FILE_FORMAT = my_parquet_format; $$
      FROM
        TABLE (
          INFER_SCHEMA(
            LOCATION => '@citibike_stage/Data',
            FILE_FORMAT => 'citibike_parquet_ff'
          )
        )
    );

-- Output of the command above
--
-- CREATE OR REPLACE EXTERNAL TABLE FOO ("BIRTH_YEAR" NUMBER(4, 0) AS ($1:BIRTH_YEAR::NUMBER(4, 0)),
-- "PROGRAM_ID" NUMBER(4, 0) AS ($1:PROGRAM_ID::NUMBER(4, 0)),
-- "TRIPDURATION" NUMBER(9, 0) AS ($1:TRIPDURATION::NUMBER(9, 0)),
-- "START_STATION_ID" NUMBER(4, 0) AS ($1:START_STATION_ID::NUMBER(4, 0)),
-- "STOPTIME" TIMESTAMP_NTZ AS ($1:STOPTIME::TIMESTAMP_NTZ),
-- "END_STATION_ID" NUMBER(4, 0) AS ($1:END_STATION_ID::NUMBER(4, 0)),
-- "GENDER" NUMBER(2, 0) AS ($1:GENDER::NUMBER(2, 0)),
-- "USERTYPE" TEXT AS ($1:USERTYPE::TEXT),
-- "STARTTIME" TIMESTAMP_NTZ AS ($1:STARTTIME::TIMESTAMP_NTZ),
-- "BIKEID" NUMBER(9, 0) AS ($1:BIKEID::NUMBER(9, 0)))
-- LOCATION=@citibike_stage/Data
-- FILE_FORMAT='citibike_parquet_ff';

CREATE OR REPLACE EXTERNAL TABLE TRIPS_BIG_XT (
"BIRTH_YEAR" NUMBER(4, 0) AS ($1:BIRTH_YEAR::NUMBER(4, 0)),
"PROGRAM_ID" NUMBER(4, 0) AS ($1:PROGRAM_ID::NUMBER(4, 0)),
"TRIPDURATION" NUMBER(9, 0) AS ($1:TRIPDURATION::NUMBER(9, 0)),
    STARTDATE    date as
    to_date(split_part(metadata$filename, '/', 3) || '-' || split_part(metadata$filename, '/', 4) || '-01'),
"START_STATION_ID" NUMBER(4, 0) AS ($1:START_STATION_ID::NUMBER(4, 0)),
"STOPTIME" TIMESTAMP_NTZ AS ($1:STOPTIME::TIMESTAMP_NTZ),
"END_STATION_ID" NUMBER(4, 0) AS ($1:END_STATION_ID::NUMBER(4, 0)),
"GENDER" NUMBER(2, 0) AS ($1:GENDER::NUMBER(2, 0)),
"USERTYPE" TEXT AS ($1:USERTYPE::TEXT),
"STARTTIME" TIMESTAMP_NTZ AS ($1:STARTTIME::TIMESTAMP_NTZ),
"BIKEID" NUMBER(9, 0) AS ($1:BIKEID::NUMBER(9, 0))
)
partition by (startdate)
    location = @citibike_stage/Data
    auto_refresh = false
    file_format=(format_name=citibike_parquet_ff);

-- 2.1.6  Let’s see what the data looks like in the new
--        external table.
select * from trips_big_xt limit 100;

-- 2.1.7  Add the partition column into the query for effective pruning

select
  start_station_id,
  count(*) num_trips,
  avg(tripduration) avg_duration
from trips_big_xt
where startdate between to_date('2014-01-01') and to_date('2014-06-30')
group by 1;

-- 2.1.8  External tables act like regular tables,
--        so we can join them with other tables

with t as (
  select
     start_station_id,
     end_station_id,
     count(*) num_trips
  from trips_big_xt
  where startdate between to_date('2014-01-01') and to_date('2014-12-30')
    group by 1, 2)

select
   ss.station_name start_station,
   es.station_name end_station,
   num_trips
 from t inner join stations_vw ss on t.start_station_id = ss.station_id
        inner join stations_vw es on t.end_station_id = es.station_id
order by 3 desc;

/* *********************************************************************************** */
/* *** MODULE 3  ********************************************************************* */
/* *********************************************************************************** */

--  3.1.1  Create the materialized view

use role SYSADMIN;
use schema DEMO;

create or replace materialized view TRIPS_MV as
select
  startdate,
  start_station_id,
  end_station_id,
  count(*) num_trips
from trips_big_xt
group by 1, 2, 3;

select
  count(*) 		num_rows,
  sum(num_trips) 	num_trips
from trips_mv;

-- 3.1.2 Let’s re-run our join query, replacing the
--       external table with the new materialized view.

with t as (
  select
    start_station_id,
    end_station_id,
    sum(num_trips) num_trips
  from trips_mv
  where startdate between to_date('2014-01-01') and to_date('2014-12-30')
  group by 1, 2)

select
  ss.station_name start_station,
  es.station_name end_station,
  num_trips
from t
  inner join stations_vw ss
     on t.start_station_id = ss.station_id
  inner join stations_vw es
     on t.end_station_id = es.station_id
order by 3 desc;


/* *********************************************************************************** */
/* *** MODULE 4  ********************************************************************* */
/* *********************************************************************************** */

-- 4.1.1 Create an external stage where documents are stored.

create or replace stage documents
 url = 's3://sfquickstarts/VHOL Snowflake for Data Lake/PDF/'
 directory = (enable = true auto_refresh = false);

alter stage documents refresh;

ls @documents;

-- 4.1.2 search the Directory Table for files with algorithm in
--       the name

select *
from directory(@documents)
where file_url ilike '%algorithm%pdf';

--4.2.1 Query directory table using scoped URL

select
  build_scoped_file_url(@documents, relative_path) as scoped_file_url
from directory(@documents);

-- 4.2.1 Create external stage to import PDFBox from S3
create or replace stage jars_stage
 url = 's3://sfquickstarts/Common JARs/'
 directory = (enable = true auto_refresh = false);

-- 4.2.2 Create a java function to parse PDF files
create or replace function read_pdf(file string)
returns string
language java
imports = ('@jars_stage/pdfbox-app-2.0.24.jar')
HANDLER = 'PdfParser.ReadFile'
as
$$
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class PdfParser {

    public static String ReadFile(InputStream stream) throws IOException {
        try (PDDocument document = PDDocument.load(stream)) {

            document.getClass();

            if (!document.isEncrypted()) {

                PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                stripper.setSortByPosition(true);

                PDFTextStripper tStripper = new PDFTextStripper();

                String pdfFileInText = tStripper.getText(document);
                return pdfFileInText;
            }
        }

        return null;
    }
}
$$;

-- 4.2.3 Next, you can query the table and use the udf to parse the contents of
-- the PDF as unstructured data.

select
  relative_path,
  file_url,
  read_pdf('@documents/' || relative_path)
from directory(@documents)
limit 5;
