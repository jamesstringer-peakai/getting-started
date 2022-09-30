## Install the connector you want to use 
install.packages(‘DBI’)
install.packages(‘odbc’)
install.packages("tidyverse")

# Load the packages you need
library(DBI)
library(odbc)
library(tidyverse)

# Create a connection string using the `odbc` file within a Workspace
conn <- dbConnect(odbc(), paste0(Sys.getenv('TENANT') , '-' , Sys.getenv('STAGE')))

# Pull the data from Snowflake into a Workspace
df <- dbGetQuery(conn, “select * from STAGE.HOUSEPRICES”)
