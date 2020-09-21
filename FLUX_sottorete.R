

rm(list = ls())

library(RPostgreSQL)
library(readr)
library(lubridate)
library(threadr)
library(stringr)
library(ggplot2)
library(dplyr)
library(openair)
library(pander)
library(ggrepel)
library(grid)
library(gridExtra)
library(broom)
library(tidyr)
library(RColorBrewer)
library(ggpmisc)
library(varhandle)
options(scipen=5)


setwd("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data")

## load all sottorete
data <- read_csv("sottorete_counts_timdedate.csv")[-1]

## get the hour
data <- data %>%
  mutate(hour = hour(timedate))
data <- as.data.frame(data)
data$hour <- as.factor((data$hour))



##########################################################
#### compute the FLUX of the sottorete ###################
##########################################################

## get all passaggi (conteggi) by hour
## resample data  by 15 minutes

data <- data %>%
  group_by(Date=floor_date(timedate, "15 minute"), hour, u, v) %>%
  summarize(travel_speed = mean(travel_speed),
            counts = mean(counts))

## remove data from memory
# rm(data)


data <- as.data.frame(data)

## calculate the FLUX
passaggi_hourly <- data %>%
  group_by(u,v, hour) %>%
  summarise(flux = mean((counts)/0.25, na.rm = TRUE),
            travel_speed = mean(travel_speed, na.rm = TRUE))
passaggi_hourly <- as.data.frame(passaggi_hourly)

## save data
write.csv(passaggi_hourly, "FLUX_sottorete_CATANIA.csv")


rm(data)
## clear memory
gc()

