

rm(list = ls())

library(RPostgreSQL)
library(readr)
library(lubridate)
library(threadr)
library(stringr)
library(ggplot2)
library(dplyr)
library(openair)
library(gsubfn)
library(mgsub)
library(data.table)


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


setwd("D:/ENEA_CAS_WORK/Catania_RAFAEL/viasat_data")


###############################################################################
###############################################################################

## load accuracy data from DB
###  ACCURACY: ([length of the matched trajectory] / [length of the travelled distance (sum  delta progressives)])*100

# accuracy_2019_all = dbGetQuery(conn_HAIG, "
#                                     SELECT *
#                                     FROM accuracy_2019
#                                     ")

accuracy_2019 = dbGetQuery(conn_HAIG, "
                                    SELECT accuracy
                                    FROM accuracy_2019
                                    ")

accuracy_2019$accuracy <- as.numeric(accuracy_2019$accuracy)

accuracy_2019 <- accuracy_2019 %>%
  filter(accuracy > 0 & accuracy < 100)

df_accuracy_2019_CT <- accuracy_2019

### summary STATS (80%-120% accuracy) #######################
#############################################################
grouped_accuracy <- accuracy_2019 %>%
  group_by(accuracy) %>%
  summarise(count = length(accuracy))
TOTAL = sum(grouped_accuracy$count)
good_interval <- grouped_accuracy %>%
  filter(accuracy >= 80 & accuracy <= 120)
GOODNESS_matching <- round((sum(good_interval$count) / TOTAL)*100 , digits = 2)
#############################################################
#############################################################



### plot a distribution
p <- ggplot(accuracy_2019, aes(x = accuracy)) +
  geom_histogram(binwidth = 10) +  # 25
  # geom_histogram() +
  theme_bw() 
p



### plot a distribution
p <- ggplot(accuracy_2019, aes(x = accuracy)) +
  geom_histogram(binwidth = 8) +  # 8
  # geom_histogram() +
  theme_bw() +
  # geom_density(stat = 'bin') +
  theme(legend.title=element_blank()) + 
  aes(y=stat(count)/sum(stat(count))) + 
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme( strip.text = element_text(size = 13)) +
  guides(fill=FALSE) +
  theme(axis.text.x=element_text(angle=0,hjust=0.5,vjust=1, size=12)) +
  theme(axis.text.x=element_text(size=12, colour = "black")) +
  theme(axis.title.x = element_text(face="bold", colour="black", size=13)) +
  xlab("accuracy (%) = length matched path / length travelled distance") +
  ylab("frequenza (%)") +
  xlim(0,100)+
  geom_vline(xintercept = 90, col="blue", lty=2, size=1) +
  geom_vline(xintercept = 80, col="blue", lty=2, size=1) +
  geom_vline(xintercept = 100, col="gray", lty=2, size=1) +
  geom_vline(xintercept = 110, col="gray", lty=2, size=1) +
  geom_text(aes(x = 85 , y = 0.2 , label = "90%"), size = 3) +
  geom_text(aes(x = 75 , y = 0.2 , label = "80%"), size = 3) +
  geom_vline(xintercept = 120, col="red", lty=2, size=1) +
  geom_text(aes(x = 130 , y = 0.2 , label = "120%"), size = 3) +
  geom_text(aes(x = 100 , y = 0.2 , label = "100%"), size = 3) +
  geom_text(aes(x = 110 , y = 0.2 , label = "110%"), size = 3) +
  theme(axis.title.y = element_text(face="bold", colour="black", size=13),
        axis.text.y  = element_text(angle=0, vjust=0.5, size=13, colour="black")) +
  ggtitle("Accuracy map-matching 2019 Viasat - Catania") +
  theme(plot.title = element_text(lineheight=.8, face="bold", size = 13))
p


#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################
#########################################################################

#### check Salerno ######################################################

conn_HAIG <- dbConnect(drv, dbname = "HAIG_Viasat_SA",
                       host = "10.0.0.1", port = 5432,       
                       user = "postgres", password = "superuser")


setwd("D:/ENEA_CAS_WORK/SENTINEL/viasat_data")


# all_trips <- fread("all_TRIP_IDs_2019.txt")

###############################################################################
###############################################################################

## load accuracy data from DB
###  ACCURACY: ([length of the matched trajectory] / [length of the travelled distance (sum  delta progressives)])*100

# accuracy_2019_all = dbGetQuery(conn_HAIG, "
#                                     SELECT *
#                                     FROM accuracy_2019
#                                     ")

accuracy_2019 = dbGetQuery(conn_HAIG, "
                                    SELECT accuracy
                                    FROM accuracy_2019
                                    ")

accuracy_2019$accuracy <- as.numeric(accuracy_2019$accuracy)

accuracy_2019 <- accuracy_2019 %>%
  filter(accuracy > 0 & accuracy < 100)


df_accuracy_2019_SA <- accuracy_2019

### summary STATS (80%-120% accuracy) #######################
#############################################################
grouped_accuracy <- accuracy_2019 %>%
  group_by(accuracy) %>%
  summarise(count = length(accuracy))
TOTAL = sum(grouped_accuracy$count)
good_interval <- grouped_accuracy %>%
  filter(accuracy >= 80 & accuracy <= 120)
GOODNESS_matching <- round((sum(good_interval$count) / TOTAL)*100 , digits = 2)
#############################################################
#############################################################


### plot a distribution
p <- ggplot(accuracy_2019, aes(x = accuracy)) +
  geom_histogram(binwidth = 10) +  # 25
  # geom_histogram() +
  theme_bw() 
p



### plot a distribution
p <- ggplot(accuracy_2019, aes(x = accuracy)) +
  geom_histogram(binwidth = 8) +  # 25   # 20
  # geom_histogram() +
  theme_bw() +
  # geom_density(stat = 'bin') +
  theme(legend.title=element_blank()) + 
  aes(y=stat(count)/sum(stat(count))) + 
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme( strip.text = element_text(size = 13)) +
  guides(fill=FALSE) +
  theme(axis.text.x=element_text(angle=0,hjust=0.5,vjust=1, size=12)) +
  theme(axis.text.x=element_text(size=12, colour = "black")) +
  theme(axis.title.x = element_text(face="bold", colour="black", size=13)) +
  xlab("accuracy (%) = length matched path / length travelled distance") +
  ylab("frequenza (%)") +
  geom_vline(xintercept = 90, col="blue", lty=2, size=1) +
  geom_vline(xintercept = 80, col="blue", lty=2, size=1) +
  geom_vline(xintercept = 100, col="gray", lty=2, size=1) +
  geom_vline(xintercept = 110, col="gray", lty=2, size=1) +
  geom_text(aes(x = 85 , y = 0.2 , label = "90%"), size = 3) +
  geom_text(aes(x = 75 , y = 0.2 , label = "80%"), size = 3) +
  geom_vline(xintercept = 120, col="red", lty=2, size=1) +
  geom_text(aes(x = 130 , y = 0.2 , label = "120%"), size = 3) +
  geom_text(aes(x = 100 , y = 0.2 , label = "100%"), size = 3) +
  geom_text(aes(x = 110 , y = 0.2 , label = "110%"), size = 3) +
  theme(axis.title.y = element_text(face="bold", colour="black", size=13),
        axis.text.y  = element_text(angle=0, vjust=0.5, size=13, colour="black")) +
  ggtitle("Accuracy map-matching 2019 Viasat - Salerno") +
  theme(plot.title = element_text(lineheight=.8, face="bold", size = 13))
p



##############################################################################
##############################################################################

### join all ratios together....

names(df_accuracy_2019_CT) <- "accuracy"
df_accuracy_2019_CT$source <- "Catania"


names(df_accuracy_2019_SA) <- "accuracy"
df_accuracy_2019_SA$source <- "Salerno"


df <- rbind(df_accuracy_2019_CT, 
            df_accuracy_2019_SA)
somma_df <- df %>%
  group_by(source) %>%
  summarise(total = length(accuracy))

  
  
  p <- ggplot(df) + 
  aes(x =source, y = accuracy) +
  geom_boxplot(fill = "orange") + 
  theme_bw() +
  theme(axis.title.x = element_text(face="bold", colour="black", size=13),     
        axis.text.x=element_text(angle=0,hjust=0.5,vjust=1, size=14)) +
  ylab("frequenza (%)") +
  xlab("accuratezza") +
  theme(axis.title.y = element_text(face="bold", colour="black", size=13),
        axis.text.y  = element_text(angle=0, vjust=0.5, size=13, colour="black")) +
  geom_text(data = somma_df, aes(x = source, y = 2, label = total), size = 4, hjust = 1.5,  vjust = -0.5)+
  ggtitle("accuratezza map-matching su Catania e Salerno") +
  theme(plot.title = element_text(lineheight=.8, face="bold", size = 13))
  p

###############################################################################
###############################################################################


