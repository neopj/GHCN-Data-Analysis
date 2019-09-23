# import packages
library(dplyr)
library(readr)
library(ggplot2)
library(plotly)
library(countrycode)
# set work path
mydir = "~/Documents/Master of Data science/420 scalable data/cummu_rainfall_country.csv"

# load all csv files from dialy_NZ and add column names
cummu_rainfall <- list.files(path = mydir, full.names = TRUE) %>%
  lapply(function(i){
    read_csv(i, col_names = c("COUNTRYCODE","COUNTRYNAME","AVG_RAINFALL")
    )}
  ) %>% 
  bind_rows()

# glimpse the dataframe
head(cummu_rainfall,30)
dim(cummu_rainfall)

# modify the countrycode from ISO A2 code to ISO A3 code
rainfall_world <- cummu_rainfall %>%
  mutate(COUNTRYCODE = countrycode(COUNTRYCODE,"iso2c","iso3c"))


# plot the average rainfall of the world
rainfall_world_plot <- plot_geo(rainfall_world) %>%
  add_trace(
    z = ~AVG_RAINFALL,
    color = ~AVG_RAINFALL,
    colors = 'Reds',
    text = ~COUNTRYNAME, 
    locations = ~COUNTRYCODE
    ) %>%
  layout(
    title = "Average rainfall of the world",
    geo = list(
      scope = "world",
      showland = TRUE,
      landcolor = toRGB("gray80")
    ))
   
rainfall_world_plot



