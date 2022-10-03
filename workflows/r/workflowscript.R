# Import packages that you need 
library(dplyr)
library(odbc)
library(DBI)
library(tidymodels)

# Setting some variables
bucket = Sys.getenv('DATA_LAKE') # For the data lake connection
filepath = paste0(Sys.getenv('DATA_LAKE_ROOT_PATH') , 'datascience/') # For the data lake connection
modelname = 'model.rds' # Model name

# Connection string for Snowflake
print("Connecting to Snowflake")
conn = dbConnect(odbc::odbc(),
                 driver = '/usr/lib/snowflake/odbc/lib/libSnowflake.so',
                 server = paste0('peak-', Sys.getenv('TENANT'),'.snowflakecomputing.com'),
                 database = Sys.getenv('SNOWFLAKE_DATABASE'),
                 uid = Sys.getenv('SNOWFLAKE_USERNAME'),
                 pwd = Sys.getenv('SNOWFLAKE_PASSWORD')
)

# Query the raw data and filter out house prices 
print("Get data and filter price")
df = dbGetQuery(conn, "select * from STAGE.HOUSE_PRICES") %>%
  filter(price > 0)

# Removing categorical variables
print("Only select numerical data")
data = df %>%
  select(-date,
         -city,
         -PEAKAUDITCREATEDAT, 
         -REQUEST_ID)

# Building and fitting Linear Regression model. You can change this
print("Build model")
linreg_reg_spec <- 
  linear_reg() %>% 
  set_engine("lm")

linreg_reg_fit <- linreg_reg_spec %>% fit(price ~ ., data = data)

# Predicting on our data and creating a column called predictions
predictions = df %>%
  add_column(predict(linreg_reg_fit, data))  

names(predictions)[18] <- 'predictions'

# Save the model to the data lake using the AWS CLI functionality
print("Save model to S3")
saveRDS(linreg_reg_fit, 'model.rds')
system(glue::glue("aws s3 cp 'model.rds' 's3://{bucket}/{filepath}{modelname}'"))

# Changing the data types so that we can save the data to Snowflake
print("Altering the data")
predictions = predictions %>%
  mutate(price = as.integer(price), 
          predictions = as.integer(predictions), 
          bedrooms = as.integer(bedrooms), 
          bathrooms = as.integer(bathrooms), 
          floors = as.integer(floors) ) %>%
  select(-PEAKAUDITCREATEDAT, 
         -REQUEST_ID)

# Changing the column names to upper case so that we can save to Snowflake
names(predictions) <- toupper(names(predictions))

# Creating two queries to create and delete from the tables  
print("Save outputs to Snowflake")
create_table_query = "
CREATE OR REPLACE TABLE PUBLISH.HOUSEPRICE_PREDICTIONS (
date varchar(256),
  price integer,
  bedrooms integer,
  bathrooms integer,
  sqftliving integer,
  sqftlot integer,
  floors integer,
  waterfront integer,
  view integer,
  condition integer,
  sqftabove integer,
  sqftbasement integer,
  yrbuilt integer,
  yrrenovated integer,
  city varchar(256),
  predictions integer    
)

"
delete_query = "delete from PUBLISH.HOUSEPRICE_PREDICTIONS"

# Using DBI to run the queries on Snowflake
dbSendQuery(conn, create_table_query)
dbSendQuery(conn, delete_query)

print("Saving data to Snowflake")

# Appending to the Snowflake Table
dbAppendTable(conn, SQL("PUBLISH.HOUSEPRICE_PREDICTIONS"), predictions)
print("Complete")
