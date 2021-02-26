
rm(list = ls())

library(stringr)
library(dplyr)
library(threadr)
library(readr)
library(gstat)
library(ggplot2)

options(warn=-1)

setwd("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data")

##########################################################################
#### get all loads (on the EDGES) from Viadotto San Paolo, NO CLOSURE ####
##########################################################################

## "Viadotto San Paolo" u,v --> (841721621, 6758675255) # Descending
## "Viadotto San Paoo" u,v --> (4096452579, 6758779932) # Ascending

# df_open <- read_csv('LOADS_Viadotto_SanPaolo_CATANIA_21_NOVEMBER_2019.csv')[-1]
df_open_CATANIA <- read_csv('LOADS_Viadotto_SanPaolo_CATANIA_only_21_NOVEMBER_2019.csv')[-1]
df_open_ACIREALE <- read_csv('LOADS_Viadotto_SanPaolo_ACIREALE_only_21_NOVEMBER_2019.csv')[-1]
## sort by largest load(%)
df_open_CATANIA <- df_open_ACIREALE[order(-df_open_ACIREALE$`load(%)`),]
df_open_ACIREALE <- df_open_ACIREALE[order(-df_open_ACIREALE$`load(%)`),]

# df_closed_ascending <- read.csv('DISRUPTION_Viadotto_SanPaolo_verso_ACIREALE_21_NOVEMBER_2019.csv')
df_closed_ACIREALE <- read.csv('LOADS_DISRUPTION_Viadotto_SanPaolo_ACIREALE_only_21_NOVEMBER_2019.csv')
### rename one column
colnames(df_closed_ACIREALE)[colnames(df_closed_ACIREALE) == "load..."] <- 'loads(%)'
df_closed_ACIREALE <- df_closed_ACIREALE %>%
  select(u,v,`loads(%)`)

# df_closed_descending <- read.csv('DISRUPTION_Viadotto_SanPaplo_verso_CATANIA_21_NOVEMBER_2019.csv') 
### rename one column
df_closed_CATANIA <- read.csv('LOADS_DISRUPTION_Viadotto_SanPaolo_CATANIA_only_21_NOVEMBER_2019.csv')
### rename one column
colnames(df_closed_CATANIA)[colnames(df_closed_CATANIA) == "load..."] <- 'loads(%)'
df_closed_CATANIA <- df_closed_CATANIA %>%
  select(u,v,`loads(%)`)
## sort by largest load(%)
df_closed_CATANIA <- df_closed_CATANIA[order(-df_closed_CATANIA$`loads(%)`),]
df_closed_ACIREALE <- df_closed_ACIREALE[order(-df_closed_ACIREALE$`loads(%)`),]


  
## load EDGE files with all info of the road network over Catania
EDGES <- read.csv("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data/gdf_edges.csv")[-1]
EDGES <- as.data.frame(EDGES)
## skip "geometry"
EDGES <- EDGES[, names(EDGES)[which(names(EDGES) != "geometry")]]
# remove duplicates of u & v
EDGES <- EDGES[-1] %>%
  distinct(u, v, .keep_all = TRUE) 

## join EDGES with df_open_CATANIA
df_open_CATANIA <- df_open_CATANIA %>%
  left_join(EDGES, by = c("u", "v"))

## join EDGES with df_open_ACIREALE
df_open_ACIREALE <- df_open_ACIREALE %>%
  left_join(EDGES, by = c("u", "v"))

## join EDGES with df_open
df_closed_ascending <- df_closed_ascending %>%
  left_join(EDGES, by = c("u", "v"))

## join EDGES with df_closed_CATANIA
df_closed_CATANIA <- df_closed_CATANIA %>%
  left_join(EDGES, by = c("u", "v"))

## join EDGES with df_closed_ACIREALE
df_closed_ACIREALE <- df_closed_ACIREALE %>%
  left_join(EDGES, by = c("u", "v"))


#############################################
### filter up 51% and then unique.....

## categories <- as.character( unique(STATS_passaggi_daily$classi))

