

library(magick)
library(stringr)
# remotes::install_github("rstudio/chromote")
# remotes::install_github("rstudio/webshot2")
library(webshot2)
library(lubridate)

rm(list = ls())
# setwd("C:/Users/karaf/ownCloud/Catania_RAFAEL/viasat_data/congestion/maps")
# setwd("C:/Users/karaf/ownCloud/Catania_RAFAEL/viasat_data/criticality")
# setwd("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/criticality")
# setwd("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/congestion/maps")
setwd("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/resilience")

# patt <- "html"
# filenames <- list.files(pattern = patt)
# 
# 
# ## take a snapshot from each HTML file and make a .png file (image), save with date label
# for (i in 1:length(filenames)) {
#   print(i)
#   date <- str_sub(filenames[i], start = 13, end = -27)
#   ### criticality
#   webshot(filenames[i], 
#           file = paste0("criticality_CATANIA_", date, ".png"), zoom = 3, cliprect = c(250, 200, 450, 450),
#           vwidth = 900, vheight = 900) 
#   
# }


images <- list.files(pattern = ".png")
# dir.create("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/criticality/movie")
# output_dir <- "C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/criticality/movie/"

# dir.create("C:/Users/karaf/ownCloud/Catania_RAFAEL/viasat_data/criticality/movie")
# output_dir <- "C:/Users/karaf/ownCloud/Catania_RAFAEL/viasat_data/criticality/movie/"

# dir.create("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/congestion/maps/movie")
# output_dir <- "C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/congestion/maps/movie/"

dir.create("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/resilience/movie")
output_dir <- "C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/resilience/movie/"


#### DAILY IMAGES with labels for CONGESTION and CRITICALITY 
for (i in 1:length(images)) {
  print(i)
  ## open .png images
  img <- image_read(images[i])
  # plot(img)
  ## add LABEL with DATE
  date <- str_sub(images[i], start = 12, end = -26)
  ## get DAY of the WEEK
  date <- as.Date(date)
  # weekday <- as.character(wday(date, label=TRUE))
  weekday <- weekdays(as.Date(date))
  # month <- as.character(month(date, label=TRUE))
  month <- toupper(months(date))
  # label <- paste0(weekday, " ", date)
  label <- paste0(month, " --> ", weekday)
  img <- image_annotate(img, label, size = 25, color = "black", boxcolor = "transparent",
                        degrees = 0, location = "+130+570")   ## +155+270 
  plot(img)  
  image_write(img, paste0(output_dir, "congestion_", date, "_Catania_all_vehicles.png"), format = "png")
  
}



#### MONTHLY IMAGES with labels for RESILIENCE
for (i in 1:length(images)) {
  print(i)
  ## open .png images
  img <- image_read(images[i])
  # plot(img)
  ## add LABEL with MONTH
  MONTH <- str_sub(images[i], start = 12, end = -31)
  month <- toupper(MONTH)
  label <- paste0(month, " 2019")
  img <- image_annotate(img, label, size = 25, color = "black", boxcolor = "transparent",
                        degrees = 0, location = "+130+570")   ## +155+270 
  # plot(img)  
  image_write(img, paste0(output_dir, "resilience_", month, "_Catania_all_vehicles.png"), format = "png")
  
}


# to make a movie.......
# to use with ImageMagik using the commnad line cmd in windows
# cd into the directory where there are the png files

# magick -delay 100 -loop 0 *.png MOVIE_CRITICALITY_Catania_Feb_May_Aug_Nov_2019.gif

###########################################################################
###########################################################################
###########################################################################






