library(sf)
library(plyr)
library(dplyr)
library(stars)
library(raster)
library(rriskDistributions)
library(EnvStats)

#load municipalities
VG_GEM<-st_read("VG250_GEM.shp")

#load model
load("IRC model.R") 

#co-variables
Rn_S<-st_as_stars(raster("RnSoil 2_0.tif"))
Rn_O<-st_as_stars(raster("RnOut 1_0.tif"))
Perm<-st_as_stars(raster("Permeability 2_0.tif"))
Temp<-st_as_stars(raster("Temperature 1981-2010.tif"))
Temp<-st_warp(Temp,Rn_S)
Prec<-st_as_stars(raster("Precipitation 1981-2010.tif"))
Prec<-st_warp(Prec,Rn_S)
SM<-st_as_stars(raster("Soil Moisture 1981-2010.tif"))
SM<-st_warp(SM,Rn_S)
slope<-st_as_stars(raster("Slope 25.tif"))
faults<-st_as_stars(raster("tectonic fault density 100m.tif"))
WindExp<-st_as_stars(raster("wind expsoure DGM25.tif"))
st_crs(WindExp)<-25832


library(foreach)
n.cores <- parallel::detectCores() -1

#create the cluster
my.cluster <- parallel::makeCluster(
  n.cores, 
  type = "PSOCK"
)
#register it to be used by %dopar%
doParallel::registerDoParallel(cl = my.cluster)

#check if it is registered (optional)
foreach::getDoParRegistered()

#how many workers are available? (optional)
foreach::getDoParWorkers()


#chunk-wise calculation
foreach(
  j = c(1:4400),
  .packages = c("sf","plyr","dplyr","stars","raster","rriskDistributions","EnvStats")
) %dopar% {
  
  #load building data in chunks of 5000 buildings
  EW_data <- st_read("Haushalte_Einwohner.gpkg",  #Haushalte_Einwohner.gpkg is the building data set with ~22 million German building 
                     query=paste("select * from Haushalte_Einwohner LIMIT ",5000, "OFFSET ",(j-1)*5000))
  
  Est_data<-EW_data[,c("ID","Strasse","Hn","PLZ","Ort","Einwohner","Haushalte","Etagen","Baujahrsklasse",
                       "Charakteristik")]
  
  #select only buildings with inhabitants
  Est_data<- Est_data %>% filter(Einwohner>0)
  
  #data extraction
  RnS.ex<-st_extract(Rn_S,Est_data)
  Perm.ex<-st_extract(Perm,Est_data)
  Temp.ex<-st_extract(Temp,Est_data)
  Prec.ex<-st_extract(Prec,Est_data)
  SM.ex<-st_extract(SM,Est_data)
  slope.ex<-st_extract(slope,Est_data)
  faults.ex<-st_extract(faults,Est_data)
  WindExp.ex<-st_extract(WindExp,Est_data)
  
  Est_data$RnS <- RnS.ex$RnSoil_2_0
  Est_data$Perm <- Perm.ex$Permeability_2_0
  Est_data$Temp <- Temp.ex$Temperature_1981.2010
  Est_data$Prec <- Prec.ex$Precipitation_1981.2010
  Est_data$SM <- SM.ex$Soil_Moisture_1981.2010
  Est_data$slope <- slope.ex$Slope_25
  Est_data$faults <- faults.ex$tectonic_fault_density_100m
  Est_data$WindExp <- WindExp.ex$wind_expsoure_DGM25
  summary(Est_data)
  
  
  #convert factor levels
  Est_data$Baujahr_Klasse<-as.factor((Est_data$Baujahrsklasse))
  Est_data$Baujahr_Klasse <- mapvalues(Est_data$Baujahr_Klasse, 
                                       from = c("1","2","3","4","5","6", "7","8","9","10","11","12"),  
                                       to = c("<1945","<1945", "1945-1980","1945-1980","1945-1980","1981-1995",
                                              "1981-1995","1996-2005","1996-2005",">2006",">2006",">2006"))
  summary(Est_data$Baujahr_Klasse)
  
  Est_data$type<-as.factor((Est_data$Charakteristik))
  Est_data$type <- mapvalues(Est_data$Charakteristik, 
                             from = c('1','2','3',"4","5","6","7","8","9"), 
                             to = c("EFH/ZFH", "RH/DHH","MFH","WB","WHH","TH","BH","Büro","Gewerbe"))
  Est_data$type<-as.factor(Est_data$type)
  summary(Est_data$type)
  
  Est_data$Wohneinheiten<-cut(Est_data$Haushalte,breaks=c(0,1,2,6,12,1000),right=TRUE)
  Est_data$Wohneinheiten <- mapvalues(Est_data$Wohneinheiten, 
                                      from = c( "(0,1]","(1,2]","(2,6]","(6,12]","(12,1e+03]"), 
                                      to = c("1 Wohneinheit","2 Wohneinheiten","3 - 6 Wohneinheiten",      
                                             "7 - 12 Wohneinheiten","13 und mehr Wohneinheiten"))
  
  #impute type
  type.NA <- Est_data %>% filter(is.na(type))
  type.NA$type[which(type.NA$Haushalte>=3)] <- as.factor("MFH")
  type.NA$type[which(type.NA$Haushalte<=2)] <-as.factor("EFH/ZFH") 
  Est_data$type[which(is.na(Est_data$type))]<-type.NA$type
  
  #impute missing floor level data
  #select empty cells
  LM<-lm(Etagen~Charakteristik+Baujahrsklasse+Haushalte,data=Est_data)
  floor.NA <- Est_data %>% filter(is.na(Etagen))
  floor.NA.filled<-predict(LM,newdata=floor.NA)
  Est_data$Etagen[is.na(Est_data$Etagen)] <- round(floor.NA.filled)
  #still missing floor level data
  floor.NA2 <- Est_data %>% filter(is.na(Etagen))
  LM2<-lm(Etagen~Haushalte,data=Est_data)
  floor.NA2.filled<-predict(LM2,newdata=floor.NA2)
  Est_data$Etagen[is.na(Est_data$Etagen)] <- round(floor.NA2.filled)
  
  #assign AGS
  join<-st_join(Est_data,VG_GEM)
  Est_data$AGS<-as.factor(join$AGS)
  AGS.NA<-Est_data[which(is.na(Est_data$AGS)),]
  if(nrow(AGS.NA)>0)
  {nearest<-st_nearest_feature(AGS.NA,VG_GEM)
  AGS_nearest<-VG_GEM[nearest,"AGS"]
  st_geometry(AGS_nearest)<-NULL
  Est_data$AGS[which(is.na(Est_data$AGS))]<-as.factor(AGS_nearest$AGS)}


  base_EFH = 0.3 # basement occupation in single- and two-family houses 
  base_MFH = 0.05 # basement occupation in multi-family house; apartment building; high-rise apartment building; terrace house; farm house; office building
  #duplicate rows based on floor level count
  i=1
  nr_fl<-Est_data$Etagen[i] #number of floor levels
  rep<-nr_fl+1 #number of duplicates
  data <- Est_data[i,] %>% slice(rep(1:n(),each=rep)) #duplication
  fl_max<-nr_fl-1  #maximum floor level
  data$fl[1:rep] <- seq(-1,fl_max,1) #fill floor level data
  EW<-Est_data$Einwohner[i] #Anzahl Einwohner
  if(Est_data$type[i] %in% c("EFH/ZFH","RH/DHH")){ #30 % weighting basement
    data$fl_EW[data$fl==-1] <- base_EFH *EW/(nr_fl+base_EFH )
    data$fl_EW[data$fl>=0] <- EW/(nr_fl+base_EFH )
  }
  if(!Est_data$type[i] %in% c("EFH/ZFH","RH/DHH")){
    data$fl_EW[data$fl==-1] <- base_MFH*EW/(nr_fl+base_MFH) #5 % weighting basement
    data$fl_EW[data$fl>=0] <- EW/(nr_fl+base_MFH)
  }
  for (i in 2:nrow(Est_data)){
    nr_fl<-Est_data$Etagen[i] #number of floor levels
    rep<-nr_fl+1 #number of duplicates
    new <- Est_data[i,] %>% slice(rep(1:n(),each=rep)) #duplication
    fl_max<-nr_fl-1 #maximum floor level
    new$fl[1:rep] <- seq(-1,fl_max,1) #fill floor level data
    EW<-Est_data$Einwohner[i] #Anzahl Einwohner
    if(Est_data$type[i] %in% c("EFH/ZFH","RH/DHH")){ #30 % weighting basement
      new$fl_EW[new$fl==-1] <- base_EFH *EW/(nr_fl+base_EFH )
      new$fl_EW[new$fl>=0] <- EW/(nr_fl+base_EFH )
    }
    if(!Est_data$type[i] %in% c("EFH/ZFH","RH/DHH")){
      new$fl_EW[new$fl==-1] <- base_MFH*EW/(nr_fl+base_MFH) #5 % weighting basement
      new$fl_EW[new$fl>=0] <- EW/(nr_fl+base_MFH)
    }
    data<-rbind(data,new)  #merge data
    #gc()
  }
  data$fl_samples<-round(data$fl_EW*10)
  
  #convert floor levels
  data$Etage<-cut(data$fl,breaks=c(-2,-1,0,1,2,1000),right=TRUE)
  data$Etage <- mapvalues(data$Etage,from = c("(-2,-1]","(-1,0]","(0,1]","(1,2]","(2,1e+03]"), 
                          to = c("Keller","Erdgeschoss","1. Etage","2. Etage","3. Etage oder höher"))
  
  
  ###predict
  
  percentile<- c(0.1, 0.25, 0.5, 0.75,0.8,0.85,0.9,0.95,0.98) #define desired percentiles
  myQuantile <- function(y, w) quantile(rep(y, w), probs = percentile) # create quantile function
  
  #make prediction 
  pred <- predict(mod,newdata=data,type="response",FUN=myQuantile)  #make prediction
  
  #cbind data and pred
  data<-cbind(data,pred)
  
  #Outdoor Rn  -< check if RnOut is lower than 10 %ile, then set Rnout at 1 Bq/m³ lower than OutdoorRn
  RnOut.ex<-st_extract(Rn_O,data)
  data$RnOut<-RnOut.ex$RnOut_1_0
  
  data$RnOut[data$RnOut>=pred[,1]] <- pred[data$RnOut>=pred[,1],1]-1
  
  #substract threshold value
  quantile_cor<-pred-data$RnOut
  
  #fit 3 parameter lognorm distribution
  for (i in 1:nrow(data)){
    par<-get.lnorm.par(p=percentile,q=quantile_cor[i,],fit.weights=c(1,2,3,4,5,6,6,6,6),plot=FALSE,show.output = FALSE)
    data$meanlog[i]<-par[1]
    data$sdlog[i]<-par[2]
  }
  write_sf(data,paste0("Floor level predictions_",j,".gpkg"))

  #draw samples
  final_predictions<-data.frame(matrix(nrow=sum(data$fl_samples),ncol=3))
  colnames(final_predictions)<-c("AGS","Ort","IRC Sample")
  for (i in 1:nrow(data)){
    if(data$fl_samples[i]>0){
      samples<-rlnorm(n=data$fl_samples[i],mean=data$meanlog[i],sd=data$sdlog[i])
      samples_cor<-samples+data$RnOut[i]
      if(i==1){start_position<-1}
      if(i>1){start_position<-sum(data$fl_samples[1:(i-1)])+1}
      end_position<-start_position-1+data$fl_samples[i]
      final_predictions[start_position:end_position,1]<-rep(as.character(data$AGS[i]),data$fl_samples[i])
      final_predictions[start_position:end_position,2]<-rep(data$Ort[i],data$fl_samples[i])
      final_predictions[start_position:end_position,3]<-as.numeric(round(samples_cor))
    }
  }
  write.csv(final_predictions,paste0("Final predictions_chunk_",j,".csv"))
}
parallel::stopCluster(cl = my.cluster)
