

rm(list = ls())

library(RPostgreSQL)
library(lubridate)
library(threadr)
library(stringr)
library(ggplot2)
library(dplyr)
library(openair)
library(gsubfn)
library(mgsub)


setwd("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data")

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")

# Connection to postdev-01 server where DB with TomTom data from Gibraltar is stored
# conn_HAIG <- dbConnect(drv, dbname = "HAIG_Viasat_CT",
#                        host = "192.168.132.18", port = 5432,       
#                        user = "postgres", password = "superuser")

conn_HAIG <- dbConnect(drv, dbname = "HAIG_Viasat_CT",
                       host = "10.0.0.1", port = 5432,       
                       user = "postgres", password = "superuser")


dbListTables(conn_HAIG)
# check for the public
# dbExistsTable(conn_HAIG, "idterm_portata")
## get fields names of tables in the DB
dbListFields(conn_HAIG, "dataraw")
dbListFields(conn_HAIG, "OSM_edges")
dbListFields(conn_HAIG, "routecheck_2019")
dbListFields(conn_HAIG, "mapmatching_2019")
dbListFields(conn_HAIG, "idterm_portata")


