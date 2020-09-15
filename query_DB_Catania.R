

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

# load "idterm_vehtype_portata"
idterm_vehtype_portata <- read.csv(paste0("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/idterm_vehtype_portata.csv"),
                                   header = T, sep=",")[-1]
idterm_vehtype_portata$idterm <- as.factor(idterm_vehtype_portata$idterm)
idterm_vehtype_portata$vehtype <- as.factor(idterm_vehtype_portata$vehtype)


############################################################################################
############################################################################################
############################################################################################

### get map-matching data from selected EDGES in the CATANIA province in order to get a 
### subnetwork (sottorete) and its "idterm"

### (u,v) ---> (476455543, 4064451884),  (294034837, 6754556102), (33590390, 6761010925),
          # (2812832603, 6761045678), (832839571, 839605642),  (611623885, 293556974), 
           # (574398754, 581850989), (281306453, 6509572768), (488537136, 1767590558),
           # (637681763, 370190911), (4067921092, 290384634), (315919988,280784174),
          # (292898762, 6750351577),  (416782575, 6582460908), (574838732, 1385119966)



start_time = Sys.time()

data_sottorete =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (476455543, 4064451884),
                           (294034837, 6754556102), (33590390, 6761010925),
                           (2812832603, 6761045678), (832839571, 839605642),  
                           (611623885, 293556974),  (574398754, 581850989), 
                           (281306453, 6509572768), (488537136, 1767590558),
                           (637681763, 370190911), (4067921092, 290384634),
                           (315919988,280784174), (292898762, 6750351577), 
                           (416782575, 6582460908), (574838732, 1385119966))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff


AAA <- unique(data_sottorete[c("u", "v")])


## filter data with speel < 200 km/h
data_sottorete <- data_sottorete %>%
  filter(mean_speed < 200)

n_data <- data_sottorete %>%
  group_by(u,v) %>%
  summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
            MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
data_sottorete <- data_sottorete %>%
  left_join(idterm_vehtype_portata, by = "idterm")
write.csv(data_sottorete, "sottorete_catania.csv")

############################################################################
############################################################################

start_time = Sys.time()


SS192_km67 =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (2193083700, 4795153417),
                           (4795154225, 4795154223))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



## filter data with speel < 200 km/h
SS192_km67 <- SS192_km67 %>%
  filter(mean_speed < 200)

n_data <- SS192_km67 %>%
  group_by(u,v) %>%
  summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
            MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
SS192_km67 <- SS192_km67 %>%
  left_join(idterm_vehtype_portata, by = "idterm")
write.csv(SS192_km67, "VIASAT_SS192_km67.csv")

################################################################################
################################################################################


start_time = Sys.time()

## (305783627, 4039429604)  ---> Catania
## (281305756, 5159815955)  ---> Palermo

A19_km188 =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (281305756, 5159815955),
                           (305783627, 4039429604))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



## filter data with speel < 200 km/h
A19_km188 <- A19_km188 %>%
  filter(mean_speed < 200)

n_data <- A19_km188 %>%
  group_by(u,v) %>%
  summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
            MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
A19_km188 <- A19_km188 %>%
  left_join(idterm_vehtype_portata, by = "idterm")
write.csv(A19_km188, "VIASAT_A19_km188.csv")



################################################################################
################################################################################


start_time = Sys.time()

## (839605642, 611623893)  ---> Catania
## (611623885, 293556974)  ---> Palermo

SS121_km7 =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (839605642, 611623893),
                           (611623885, 293556974))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



## filter data with speel < 200 km/h
SS121_km7 <- SS121_km7 %>%
  filter(mean_speed < 200)

n_data <- SS121_km7 %>%
  group_by(u,v) %>%
  summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
            MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
SS121_km7 <- SS121_km7 %>%
  left_join(idterm_vehtype_portata, by = "idterm")
write.csv(SS121_km7, "VIASAT_SS121_km7.csv")


################################################################################
################################################################################


start_time = Sys.time()

## (294034837, 6754556102)  ---> Messina
## (476455543, 4064451884)  ---> Catania

RA15_km6 =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (294034837, 6754556102),
                           (476455543, 4064451884))
                                )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



## filter data with speel < 200 km/h
RA15_km6 <- RA15_km6 %>%
  filter(mean_speed < 200)

n_data <- RA15_km6 %>%
  group_by(u,v) %>%
  summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
            MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
RA15_km6 <- RA15_km6 %>%
  left_join(idterm_vehtype_portata, by = "idterm")
write.csv(RA15_km6, "VIASAT_RA15_km6.csv")

#########################################################################
#########################################################################

start_time = Sys.time()


SS114 =  dbGetQuery(conn_HAIG, "
                     WITH path AS(SELECT 
                            split_part(\"TRIP_ID\"::TEXT,'_', 1) idterm,
                            u, v,
                            timedate, mean_speed, idtrace, sequenza
                            FROM mapmatching_2019
                           WHERE (u, v) in (VALUES (302628417, 833348777),
                           (3752100124, 2558976497), (766553690, 579638795))
                     )
                             SELECT path.idterm, path.u, path.v, path.timedate,
                                    path.mean_speed, path.sequenza,
                                    \"OSM_edges\".length,
                                    \"OSM_edges\".highway,
                                    \"OSM_edges\".name,
                                    \"OSM_edges\".ref,
                                    dataraw.speed,
                                    dataraw.id
                        FROM path
                            LEFT JOIN dataraw ON path.idtrace = dataraw.id
                            LEFT JOIN \"OSM_edges\" ON path.u = \"OSM_edges\".u AND path.v = \"OSM_edges\".v  
                                ")

stop_time = Sys.time()
diff <- (stop_time - start_time)
diff



## filter data with speel < 200 km/h
SS114 <- SS114 %>%
  filter(mean_speed < 200)

n_data <- SS114 %>%
  group_by(u,v) %>%
  summarise(MEDIAN_speed = median(mean_speed, na.rm=T),
            MEDIAN_instant_speed = median(speed, na.rm=T))



## left join with "type" and "portata"
SS114 <- SS114 %>%
  left_join(idterm_vehtype_portata, by = "idterm")
write.csv(SS114, "VIASAT_SS114.csv")

