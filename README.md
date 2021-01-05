# Example Contact Tracing Using Citibike Data

## Background
At first I wanted to see if there could be any correlation between Citibike use and cases of COVID-19 spread. As I explored the data further, it provided a framework to practice implementing massive contact tracing. Using the data from Citibike, they could alert riders to and from certain locations at specific times that they were in an area of higher than usual activity and be on the look out for symptoms or to get tested at the first signs of symptoms. 

The limitations of this would be that Citibike masks the users because of GDPR compliance. Also, looking at stations specific data provides little clues as to whether a person was actually in close enough proximity to a spread event. An example is going to a Citibike station near Central Park. With that being taken account, this project does take into account how to implement taking an idea to production using data that is available. 

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

## Technology and Set Up
- Amazon S3 for file storage
- Python for data processing
- Amazon Redshift for data warehouse
- Apache Airflow for workflow managemnt and scheduling tasks

![graph_view_of_dag](https://user-images.githubusercontent.com/53429726/103698556-80744600-4f6f-11eb-86a8-cb3be83a73a3.png)

## Files 








