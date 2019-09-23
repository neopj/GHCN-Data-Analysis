# import packages
library(dplyr)
library(readr)
library(ggplot2)
library(class)

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

# Spring 1, Summer 2, Fall 3, Winter 4
seasons <- c(rep("4",2), 
              rep("1",3), 
              rep("2",3), 
              rep("3",3), 
              "4") 


ws_USA <- wind_USA %>%
  filter(ELEMENT == "AWND") %>%
  mutate(MN = as.double(substr(as.character(DATE),start=5,stop=6))) %>%
           mutate(sn = as.factor(seasons[as.double(MN)]))
  
ws_USA %>%
  head()
 


# sample randomly
sample_USA = ws_USA[sample(nrow(ws_USA),size = 500, replace = F),]
# training data is 1:300 rows
training = 1:300
train = sample_USA[training,]
# testing data is 301:500 rows.
test = sample_USA[-training,]
# plot
plot(train$sn,train$VALUE)
# set seeds
set.seed(1)
train.x = train[,"VALUE"]
test.x = test[,"VALUE"]
# built models by K-Nearest Neighbors when k = 1
knn.pred = knn(train.x,test.x,train$sn,k=1)

table(knn.pred,test$sn)
# get he mean square of error
mean(knn.pred == test$sn)
plot(knn.pred)

# built models by K-Nearest Neighbors when k = 3
knn.pred = knn(train.x,test.x,train$sn,k=3)
table(knn.pred,test$sn)
# get the mean square of error
mean(knn.pred == test$sn)
