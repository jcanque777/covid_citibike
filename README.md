# Example Contact Tracing Using Citibike Data

## Background
At first I wanted to see if there could be any correlation between Citibike use and cases of COVID-19 spread. As I explored the data further, it provided a framework to practice implementing massive contact tracing. Using the data from Citibike, they could alert riders to and from certain locations at specific times that they were in an area of higher than usual activity and be on the look out for symptoms or to get tested at the first signs of symptoms. 

The limitations of this would be that Citibike masks the users because of GDPR compliance. Also, looking at stations specific data provides little clues as to whether a person was actually in close enough proximity to a spread event. An example is going to a Citibike station near Central Park. With that being taken account, this project does take into account how to implement taking an idea to production using data that is available. 

The purpose of getting all the data together is to see if there is a relationship between citibike ridership and Covid rates. Would an increase in rides lead to an increase in case rate in the week or two to follow? If users were not masked, could we identify users that were in an area of increased activity to warn of possibility of infection due to being in a high traffic area.

## Data 
Citi Bike Data: https://s3.amazonaws.com/tripdata/202011-citibike-tripdata.csv.zip
- Citibike provides detailed account of each of their bicycles. 
- The November 2020 dataset contains 1,736,704 rows and 15 columns.

Weather Data: https://www.ncdc.noaa.gov/cdo-web/datasets/LCD/stations/WBAN:94728/detail
- Go to site above, Add To Cart, Click Cart, Choose LCD CSV Output Format, Add Email Address and Submit
- This gives us detailed hourly data from the Climatological Data Station in Central Park
- The dataset varies with the dates provided: January 1- December 12 got me 11,009 rows and 124 columns. 
- Use of this data was to show possible multicollinearity due to weather effects.

Dates:
- NYC Health does provide daily data by zip code, but does so on an aggregate level. If I wanted to be thorough I would set up a way to scrape the data every day to get a more accurate view of cases surges.
- NYC Health does show weekly data for each zip code. 
- This table simply makes it convenient to find dates without adding an extra column to the Citibike and Weather datasets

COVID: https://github.com/nychealth/coronavirus-data/blob/master/trends/caserate-by-modzcta.csv
- Weekly data with each zipcode as a column. This gets transformed to show each row having date, zipcode, and caserate
- Original table is 21 rows and 184 columns
- Transformed table after melting is 3843 rows and 3 columns.

Stations: 
- Citibike only used station name with latitide and longitude. I had to create a new table and use geopy.geocoders to convert longitude and latitude to get a zip code
- There are 1165 stations 

![Table diagram](https://user-images.githubusercontent.com/53429726/103823949-057b6000-5041-11eb-94a2-6571e2b6a9c7.png)

## Technology and Set Up
- Amazon S3 for file storage
- Python for data processing
- Amazon Redshift for data warehouse
- Apache Airflow for workflow managemnt and scheduling tasks

The main reason for using the above technologies is because they are the most popular tools for their specific purpose. At certain points in the process, I considered using other technologies to complete the project but decided that it was imperitive to use AWS for storage the data warehouse, while using Airflow as a way to manage the workflow. 

### If Data Was Increased By 100x
I would have used EMR and Spark for transformations. The beauty of using S3 and Redshift is that they scale easily and the storage and use of data warehouse would not change. 

### If the pipelines were run on a daily basis by 7am.
I would simply add "schedule_interval='0 7 * * *'" at the end of the dag statement. 

dag = DAG('copy_s3_to_redshift',
          default_args=default_args,
          description='create tables for s3 data',
          schedule_interval='0 7 * * *')

### If the database needed to be accessed by 100+ people.
Redshift is the option if 100+ people needed to access the database. The only changes would be to create a different security groups on AWS to allow different levels of access. 

![graph_view_of_dag](https://user-images.githubusercontent.com/53429726/103698556-80744600-4f6f-11eb-86a8-cb3be83a73a3.png)

## Files
In Dags Folder:
- load_data.py is the file that Airflow will use for the pipeline:
  - Get raw csv files and transform them to be loaded onto S3
  - Create Tables in Redshift
  - Load files from S3 to Redshift
- Citibike and Weather data csvs must be in data folder

In Data Folder:
- stations_table_with_zip.csv has all the stations with their corresponding zip codes 
- transformed_date.csv has all dates for 2020 with their equivalent week ending date to match NYC_health

In Plugins/Helpers Folder:
- sql_queries.sql to create tables

In Plugins/Operator Folder:
- data_quality.py checks each table in Redshift if loaded data returned empty rows
- stage_redshift.py gets data from S3 and puts into tables in redshift

## Process
1. Weather data and Citibike data must be downloaded and placed into data folder.
2a. Citibike data and Weather data are transformed and saved into data folder.
2b. Covid rates are extracted and transformed and saved into data folder.
3. Covid, Citibike, Weather, Stations, and Dates are saved to AWS S3.
4. Tables are created on AWS Redshift.
5. Data from AWS S3 is moved to AWS Redshift.
