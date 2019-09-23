# import packages
library(dplyr)
library(tidyverse)
library(readr)
library(openair)
library(circular)
# set work path
mydir = "~/Documents/Master of Data science/420 scalable data/wind_USA.csv"

# load all csv files from dialy_NZ and add column names
wind_USA <- list.files(path = mydir, full.names = TRUE) %>%
  lapply(function(i){
    read_csv(i, col_names = c("ID","DATE","ELEMENT","VALUE","YEAR","COUNTRYNAME","COUNTRCODE")
    )}
  ) %>% 
  bind_rows()

# glimpse the dataframe
head(wind_USA,10)
dim(wind_USA)

# AWDR: Average daily wind direction.
# AWND: Average daily wind speed.
# choose the stations that have the top 2 daily wind speed in 2017
wind_USA %>%
  select(ID,ELEMENT,VALUE,YEAR) %>%
  filter(ELEMENT == 'AWND', YEAR == '2017') %>%
  group_by(ID) %>%
  summarise(avg_wind = mean(VALUE)) %>%
  arrange(desc(avg_wind)) %>%
  head()
# the output is USS0006N04S, USS0006P10S

# get 2017 observations
wind_USA_2017_4s <- wind_USA %>%
  filter(YEAR == '2017', ID == 'USS0006N04S')

wind_USA_2017_0s <- wind_USA %>%
  filter(YEAR == '2017', ID == 'USS0006P10S')

# get AWND value
USS0006N04S_wind_AWND <- wind_USA_2017_4s %>%
  select(DATE,ELEMENT,VALUE) %>%
  filter(ELEMENT == 'AWND') %>%
  rename("date" = DATE,"element" = ELEMENT, "ws" = VALUE) 

# get AWDR value
USS0006N04S_wind_AWDR <- wind_USA_2017_4s %>%
  select(DATE,ELEMENT,VALUE) %>%
  filter(ELEMENT == 'AWDR') %>%
  rename("date" = DATE,"element" = ELEMENT, "wd" = VALUE)

# inner join two value to get the wind speed and direction of the same date
USS0006N04S_wind <- inner_join(USS0006N04S_wind_AWND, USS0006N04S_wind_AWDR, by = "date")
USS0006N04S_wind %>% 
  head()

# draw a wind rose
dir <- circular(USS0006P10S_wind$wd)
mag <- rgamma(USS0006P10S_wind$ws,15)
sample <- data.frame(dir=dir,mag=mag)
p1 <- windrose(sample,template='geographics')
p1

