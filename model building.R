library(raster)
library(blockCV)
library(sf)
library(sp)
library(blockCV)
library(stars)
library(plyr)
library(dplyr)
library(caret)
library(CAST)
library(party)
library(doParallel)


#load data from BKG
load("IRC_data_BKG.R") # data cannot be shared due to data protection regulations

#training indices of spatial folds
load("Cross-validation folds_IRC.R")

# defining metrics to be evaluated
mySummary <- function (data,
                       lev = NULL,
                       model = NULL) {
  out <-
    c(
      Metrics::rmse(data$obs, data$pred),
      cor(data$obs, data$pred) ^ 2,
    )
  names(out) <- c("rmse", "R2")
  out
}

spatial_blocks<-spatial_blocks[1:10]

#control parameters
fitControl <- trainControl(
  method = 'repeatedcv',            # cross validation
  number = 10,
  repeats=1,
  savePredictions = 'final',       # saves predictions for optimal tuning parameter
  summaryFunction=mySummary,  # results summary function
  index =spatial_blocks                     # indices of training data_10p over 10 folds, repeated 5 times
)


#define tuning Grid
rfGrid <-  expand.grid(mtry = 3)


#variables to exclude
vars<-c("Baujahr_Klasse","Etage","Wohneinheiten","type","RnS","Perm","Temp","Prec","SM","slope","RnOut","faults",
        "SAGAWI","WindExp","Kaufkraft")


#remove incomplete data
st_geometry(IRC.data.BKG)<-NULL
include<-which(names(IRC.data.BKG)%in%vars)

#parallel
registerDoParallel(8)
getDoParWorkers()


#forward feature selection
FFS_RF_IRC <-ffs(
  IRC.data.BKG[,include],
  IRC.data.BKG$Radon,
  metric = "rmse",
  maximize=FALSE,
  method = "cforest",
  tuneGrid = rfGrid,
  trControl = fitControl,
  controls = cforest_unbiased(ntree = 100, trace = TRUE)
) 

save(FFS_RF_IRC,file="FFS IRC.R") 
load("FFS IRC.R")


#hyperparameter grid
rfGrid<-expand.grid(mtry=seq(2,12,1))

#selected vars

st_geometry(IRC.data.BKG)<-NULL

vars<-FFS_RF_IRC$selectedvars
include<-which(names(IRC.data.BKG)%in%vars)   # find column numbers with specific column names

#feature selection ffs()
registerDoParallel(7)
getDoParWorkers()

Tune_RF_GRP <-train(
  IRC.data.BKG[,include],
  IRC.data.BKG$Radon,
  metric = "rmse",
  maximize=FALSE,
  method = "cforest",
  tuneGrid = rfGrid,
  trControl = fitControl,
  controls = cforest_unbiased(ntree = 100, trace = TRUE)
) 



#FINAL model 
ntree<-c(500)
rfGrid<-expand.grid(mtry=4)

mod <- partykit::cforest(Radon ~ ., data = IRC.data.BKG[,include],ntree=500,mtry=4,trace=TRUE)
#save(mod,file="IRC model.R") 
