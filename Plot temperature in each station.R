# Analysis Q4 (d) plot
# import pakages
library(dplyr)
library(tidyverse)
library(readr)
library(ggthemes)
library(ggpubr)
# set work path
mydir = "~/Documents/Master of Data science/420 scalable data/dialy_NZ2.csv"

# load all csv files from dialy_NZ and add column names
dailyNZ <- list.files(path = mydir, full.names = TRUE) %>%
lapply(function(i){
  read_csv(i, col_names = c("ID","STATE","COUNTRYCODE","LATITUDE","LONGITUDE",
                         "ELEVATION","STATIONNAME","GSNFLAG","HCN/CRNFLAG","WMOID","COUNTRYNAME",
                         "STATENAME","CORECOUNT","DISTINTCOUNT","OTHERCOUNT",
                         "FIRSTYEAR","LASTYEAR","DATE","ELEMENT","VALUE","MEASUREMENT",
                         "QUALITYFLAG","SOURCEFLAG","OBSERVATIONTIME","YEAR")
           )}
  ) %>% 
bind_rows()
# glimpse the dataframe
head(dailyNZ, 5)
dim(dailyNZ)

# get all minimal temperature by year and average it.
avg_min <-dailyNZ %>%
  filter(ELEMENT == 'TMIN') %>%
  group_by(YEAR) %>%
  summarise(avg_min = mean(VALUE)) %>%
  mutate(avg_min = avg_min/10)

# get all maximum temperature by year and average it.
avg_max <- dailyNZ %>%
  filter(ELEMENT == 'TMAX') %>%
  group_by(YEAR) %>%
  summarise(avg_max = mean(VALUE)) %>%
  mutate(avg_max = avg_max/10)

# join two dataframe to get the average maximun and minimun temperature of each year.
avg_temp_NZ <- inner_join(avg_min,avg_max, by = "YEAR")

# plot
p1 <- avg_temp_NZ %>%
ggplot()+
  geom_line(aes(x = YEAR, y = avg_min, colour = "TMIN")) +
  geom_line(aes(x = YEAR, y = avg_max, colour = "TMAX")) +
  scale_x_continuous(limits = c(1940, 2020), breaks = seq(1940, 2020, 10))+
  labs(title = "Average Temperature of NZ", x = "Year", y = "Temperature(℃)", labels = "Element")+
  theme_economist(base_size=10)+
  scale_colour_manual("",values = c("TMIN" = "#4169E1", "TMAX" = "#FF9912"))
p1

# to see how many stations in NZ
station_df <- dailyNZ %>%
  group_by(ID) %>%
  tally()

# define a function to plot maximum and minimum temperature of given stations
plot_station_temp <- function(station){
  Tmin_df <- dailyNZ %>%
    filter(ID == station, ELEMENT == 'TMIN') %>%
    group_by(YEAR) %>%
    summarise(Tmin = mean(VALUE)) %>%
    mutate(Tmin = Tmin/10)
  
  Tmax_df <- dailyNZ %>%
    filter(ID == station, ELEMENT == 'TMAX') %>%
    group_by(YEAR) %>%
    summarise(Tmax = mean(VALUE)) %>%
    mutate(Tmax = Tmax/10)
 
  station_Tmax_Tmin <- inner_join(Tmin_df, Tmax_df, by = "YEAR")
  
station_Tmax_Tmin %>%
    ggplot()+
    geom_line(aes(x = YEAR, y = Tmin, colour = "TMIN")) +
    geom_line(aes(x = YEAR, y = Tmax, colour = "TMAX")) +
    scale_x_continuous(limits = c(1940, 2020), breaks = seq(1940, 2020, 10))+
    labs(title = station, x = "Year", y = "Temperature(℃)")+
    theme_economist(base_size=10)+
    scale_colour_manual("",values = c("TMIN" = "#4169E1", "TMAX" = "#FF9912"))
}

#plot each station
station_list <- list(station_df[,"ID"])
station_list
for(station in station_list){
  plot_station_temp(station)
}





  
