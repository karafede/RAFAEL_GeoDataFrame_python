

library(magick)
library(stringr)
# remotes::install_github("rstudio/chromote")
# remotes::install_github("rstudio/webshot2")
library(webshot2)
library(lubridate)

rm(list = ls())
# setwd("C:/Users/karaf/ownCloud/Catania_RAFAEL/viasat_data/criticality")
setwd("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/criticality")
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
dir.create("C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/criticality/movie")
output_dir <- "C:/Users/Federico/ownCloud/Catania_RAFAEL/viasat_data/criticality/movie/"

for (i in 1:length(images)) {
  print(i)
  ## open .png images
  img <- image_read(images[i])
  # plot(img)
  ## add LABEL with DATE
  date <- str_sub(images[i], start = 13, end = -26)
  ## get DAY of the WEEK
  date <- as.Date(date)
  weekday <- as.character(wday(date, label=TRUE))
  label <- paste0(weekday, " ", date)
  img <- image_annotate(img, label, size = 30, color = "black", boxcolor = "transparent",
                        degrees = 0, location = "+165+270")
  plot(img)  
  image_write(img, paste0(output_dir, "criticality_", date, "_Catania_all_vehicles.png"), format = "png")
  
}



# to make a movie.......
# to use with ImageMagik using the commnad line cmd in windows
# cd into the directory where there are the png files

# magick -delay 100 -loop 0 *.png movie_CRITICALITY_Catania_Feb_May_Aug_Nov_2019.gif

###########################################################################
###########################################################################
###########################################################################






