library(sf)
library(plyr)
library(dplyr)
library(stars)
library(raster)
library(rriskDistributions)
library(EnvStats)
library(readr)
library(tidyverse)
library(arrow)

#reading multiple csvs
df<-data.frame()
for (i in 1:600){
  file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
  df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_1to600.csv",row.names=FALSE)

df<-data.frame()
for (i in 601:1200){
  file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
  df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_601to1200.csv",row.names=FALSE)

df<-data.frame()
for (i in 1201:1800){
  file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
  df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_1201to1800.csv",row.names=FALSE)

df<-data.frame()
for (i in 1801:2400){
  file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
  df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_1801to2400.csv",row.names=FALSE)

df<-data.frame()
for (i in 2401:3000){
  file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
  df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_2401to3000.csv",row.names=FALSE)

df<-data.frame()
for (i in 3001:3600){
file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_3001to3600.csv",row.names=FALSE)

df<-data.frame()
for (i in 3601:4374){
  file<-read.csv(paste0("Final predictions_chunk_",i,".csv"))[,c(2,4)]
  df <- rbind(df,file)
}
colnames(df)[2]<-"Rn"
df$AGS<-ifelse(nchar(df$AGS)<8,paste0("0",df$AGS),df$AGS) #add leading zero if missing
df$KR<-paste0(substr(df$AGS,1,5))
df$BL<-substr(df$AGS,1,2)
#write.csv(df,"Final predictions_3601to4374.csv",row.names=FALSE)

gc()

data<-as_tibble(data)
data$AGS<-as.character(as.integer(data$AGS))


#load all csvs
files <- list.files(pattern = glob2rx("Final predictions*.csv"))

files.read<-character(0)
for (i in 1:7){
  new<- file.path(".../Predictions",files[i])
  files.read <- c(files.read,new)
  }
data<-open_dataset(files.read,skip_rows=1,schema=schema(AGS=string(),Rn=double(),KR=string(),BL=string()),format="csv")

glimpse(data)

#national level
stat.DE <- data %>%
  summarise(Samples=n(),
            AM=mean(Rn,na.rm=TRUE),
            SD=sd(Rn,na.rm=TRUE),
            GM=exp(mean(log(Rn),na.rm=TRUE)),
            GSD=exp(sd(log(Rn),na.rm=TRUE)),
            P50=quantile(Rn,probs = 0.5,na.rm=TRUE),
            P90=quantile(Rn,probs = 0.9,na.rm=TRUE),
            P95=quantile(Rn,probs = 0.95,na.rm=TRUE),
            P99=quantile(Rn,probs = 0.99,na.rm=TRUE),
            Exc100=sum(ifelse(Rn>=100,1,0),na.rm=TRUE)/n(),#
            Exc300=sum(ifelse(Rn>=300,1,0),na.rm=TRUE)/n(),
            Exc600=sum(ifelse(Rn>=600,1,0),na.rm=TRUE)/n(),
            Exc1000=sum(ifelse(Rn>=1000,1,0),na.rm=TRUE)/n()) %>%
  collect() 

write.csv(stat.DE,"Ergebnisse DE.csv")

#municipality level
stat.GEM <- data %>%
  group_by(AGS) %>%
  summarise(Samples=n(),
            AM=mean(Rn,na.rm=TRUE),
            SD=sd(Rn,na.rm=TRUE),
            GM=exp(mean(log(Rn),na.rm=TRUE)),
            GSD=exp(sd(log(Rn),na.rm=TRUE)),
            P50=quantile(Rn,probs = 0.5,na.rm=TRUE),
            P90=quantile(Rn,probs = 0.9,na.rm=TRUE),
            P95=quantile(Rn,probs = 0.95,na.rm=TRUE),
            P99=quantile(Rn,probs = 0.99,na.rm=TRUE),
            Exc100=sum(ifelse(Rn>=100,1,0),na.rm=TRUE)/n(),#
            Exc300=sum(ifelse(Rn>=300,1,0),na.rm=TRUE)/n(),
            Exc600=sum(ifelse(Rn>=600,1,0),na.rm=TRUE)/n(),
            Exc1000=sum(ifelse(Rn>=1000,1,0),na.rm=TRUE)/n()) %>%
  collect() 

stat.GEM$AGS<-ifelse(nchar(stat.GEM$AGS)<8,paste0("0",stat.GEM$AGS),stat.GEM$AGS) #add leading zero if missing

#district level
stat.KR <- data %>%
  group_by(KR) %>%
  summarise(Samples=n(),
            AM=mean(Rn,na.rm=TRUE),
            SD=sd(Rn,na.rm=TRUE),
            GM=exp(mean(log(Rn),na.rm=TRUE)),
            GSD=exp(sd(log(Rn),na.rm=TRUE)),
            P50=quantile(Rn,probs = 0.5,na.rm=TRUE),
            P90=quantile(Rn,probs = 0.9,na.rm=TRUE),
            P95=quantile(Rn,probs = 0.95,na.rm=TRUE),
            P99=quantile(Rn,probs = 0.99,na.rm=TRUE),
            Exc100=sum(ifelse(Rn>=100,1,0),na.rm=TRUE)/n(),#
            Exc300=sum(ifelse(Rn>=300,1,0),na.rm=TRUE)/n(),
            Exc600=sum(ifelse(Rn>=600,1,0),na.rm=TRUE)/n(),
            Exc1000=sum(ifelse(Rn>=1000,1,0),na.rm=TRUE)/n()) %>%
  collect() 

stat.KR$KR<-ifelse(nchar(stat.KR$KR)<5,paste0("0",stat.KR$KR),stat.KR$KR) #add leading zero if missing

#federal states level
stat.BL <- data %>%
  group_by(BL) %>%
  summarise(Samples=n(),
            AM=mean(Rn,na.rm=TRUE),
            SD=sd(Rn,na.rm=TRUE),
            GM=exp(mean(log(Rn),na.rm=TRUE)),
            GSD=exp(sd(log(Rn),na.rm=TRUE)),
            P50=quantile(Rn,probs = 0.5,na.rm=TRUE),
            P90=quantile(Rn,probs = 0.9,na.rm=TRUE),
            P95=quantile(Rn,probs = 0.95,na.rm=TRUE),
            P99=quantile(Rn,probs = 0.99,na.rm=TRUE),
            Exc100=sum(ifelse(Rn>=100,1,0),na.rm=TRUE)/n(),#
            Exc300=sum(ifelse(Rn>=300,1,0),na.rm=TRUE)/n(),
            Exc600=sum(ifelse(Rn>=600,1,0),na.rm=TRUE)/n(),
            Exc1000=sum(ifelse(Rn>=1000,1,0),na.rm=TRUE)/n()) %>%
  collect() 

stat.BL$BL<-ifelse(nchar(stat.BL$BL)<2,paste0("0",stat.BL$BL),stat.BL$BL) #add leading zero if missing

VG_GEM<-st_as_sf(st_read("VG250_GEM.shp"))
st_geometry(VG_GEM)<-NULL
VG_KR<-st_as_sf(st_read("VG250_KRS.shp"))
st_geometry(VG_KR)<-NULL
VG_BL<-st_as_sf(st_read("VG250_LAN.shp"))
st_geometry(VG_BL)<-NULL

##spatial joining
#join GEM name
VG_GEM$AGS<-as.factor(VG_GEM$AGS)
GEM<-unique(VG_GEM[c("AGS","BEZ","GEN")])
join.GEM<-left_join(stat.GEM,GEM,by="AGS")
GEM<-join.GEM[!duplicated(join.GEM[,'AGS']),] #remove duplicate AGS (MULTIPOLYGON?)
name.GEM<-paste(join.GEM$BEZ,join.GEM$GEN)
stat.GEM$Kommune<-name.GEM

join.GEM.sf<-left_join(stat.GEM,VG_GEM,by="AGS")
join.GEM.sf<-join.GEM.sf[!duplicated(join.GEM.sf[,'AGS']),]
#write_sf(join.GEM.sf,"Ergebnisse Gemeinde_FINAL.gpkg",append=FALSE)

#join KR name
VG_KR$AGS<-as.factor(VG_KR$AGS)
KR<-unique(VG_KR[c("AGS","BEZ","GEN")])
join.KR<-left_join(stat.KR,KR,by=c("KR"="AGS"))
KR<-join.KR[!duplicated(join.KR[,'KR']),] #remove duplicate AGS (MULTIPOLYGON?)
name.KR<-paste(join.KR$BEZ,join.KR$GEN)
stat.KR$KR<-name.KR

VG_KR$KR<-paste(VG_KR$BEZ,VG_KR$GEN)
join.KR.sf<-full_join(stat.KR,VG_KR,by="KR")
#write_sf(join.KR.sf,"Ergebnisse Kreise.gpkg",append=FALSE)

#join BL name
BL<-unique(VG_BL[c("AGS","BEZ","GEN")])[1:16,]
join.BL<-left_join(stat.BL,BL,by=c("BL"="AGS"))
BL<-join.BL[!duplicated(join.BL[,'BL']),] #remove duplicate AGS (MULTIPOLYGON?)
name.BL<-join.BL$GEN
stat.BL$BL<-name.BL

join.BL.sf<-full_join(stat.BL,VG_BL,by=c("BL"="GEN"))
#write_sf(join.BL.sf,"Ergebnisse Bundesland.gpkg",append=FALSE)
