# 1* used for model testing purpose
get.CASA.ACC.raw <- function(conn){

Current.Year <- strftime(Sys.Date(),"%y")
print(get.log.msg('CASA ACC data extraction started'))
CASA.ACC.raw.query = paste("****")
  
CASA.ACC.raw <- sqlQuery(conn,CASA.ACC.raw.query)
print(dim(CASA.ACC.raw))
print(get.log.msg('CASA ACC data extraction completed'))
return(CASA.ACC.raw)
}

get.STAFF.raw <- function(conn){
  print(get.log.msg('STAFF ACC data extraction started'))
  Staff.raw <- sqlQuery(conn,"****" )
  print(dim(Staff.raw))
  print(get.log.msg('CASA ACC data extraction completed'))
  return(Staff.raw)
}

get.CASA.tx.raw <- function(conn){
  print(get.log.msg('CASA tx data extraction started'))
  strt.date <- floor_date(Sys.Date() %m-% months(4),"month")
  end.date <- floor_date(Sys.Date() %m-% months(1),"month")-days(1)
  
  CASA.tx.raw.query <-  paste("****")
  CASA.tx.raw  <- sqlQuery(conn,CASA.tx.raw.query)
  print(dim(CASA.tx.raw))
  print(get.log.msg('CASA tx data extraction completed'))
  return(CASA.tx.raw)
}

get.LN.raw <- function(conn){
  print(get.log.msg('loan data extraction started'))
  LN.raw <- sqlQuery(conn,"****")
  print(dim(LN.raw))
  print(get.log.msg('loan data extraction completed'))
  return(LN.raw)
}

get.FD.raw <- function(conn){
  print(get.log.msg('FD data extraction started'))
  FD.raw <- sqlQuery(conn,"****")
  print(dim(FD.raw))
  print(get.log.msg('FD data extraction completed'))
  return (FD.raw)
}

get.CC.raw <- function(conn){
  print(get.log.msg('CC data extraction started'))
  Current.Year <- strftime(Sys.Date(),"%y")
  CC.raw.query <- paste("****")
  CC.raw <- sqlQuery(conn,CC.raw.query)
  print(dim(CC.raw))
  print(get.log.msg('CC data extraction completed'))
  return(CC.raw)
}

get.OL.raw <- function(conn){
  print(get.log.msg('OL data extraction started'))
  strt.date <- format(floor_date(Sys.Date() %m-% months(4),"month"),"%Y%m%d")
  end.date <- format(floor_date(Sys.Date() %m-% months(1),"month")-days(1),"%Y%m%d")
  OL.qry <- paste("****")
  OL.raw <- sqlQuery(conn,OL.qry)
  print(dim(OL.raw))
  print(get.log.msg('OL data extraction completed'))
  return(OL.raw)
}

get.FB.raw <- function(){
  print(get.log.msg('FB data extraction started'))
  library("RMySQL")
  mydb<-dbConnect(MySQL(), user = 'root', password='combank@123',dbname='customerfeedback', host='localhost')
  
  FB.raw <- dbGetQuery(mydb,"****")
  print(dim(FB.raw))
  dbDisconnect(mydb)
  print(get.log.msg('FB data extraction completed'))
  return(FB.raw)
}

get.raw.data <- function(conn){
  print(get.log.msg('raw data extraction started'))
  raw.data <- NULL
  raw.data$CASA.ACC <- get.CASA.ACC.raw(conn)
  raw.data$CASA.tx <- get.CASA.tx.raw(conn)
  raw.data$LN <- get.LN.raw(conn)
  raw.data$FD <- get.FD.raw(conn)
  raw.data$CC <- get.CC.raw(conn)
  raw.data$OL <- get.OL.raw(conn)
  raw.data$FB <- get.FB.raw()
  raw.data$Staff <- get.STAFF.raw(conn)
  print(get.log.msg('RODBC connection closed'))
  print("raw data extraction completed")
  return(raw.data)
}

transformData <- function (raw.data){
  print(get.log.msg('data transformation started'))
  
  # Filtering valid NIC
  print(get.log.msg('Filtering valid NIC started'))
  raw.data$CASA.ACC$NIC <- gsub(" ","",raw.data$CASA.ACC$NIC)
  CASA.ACC <- raw.data$CASA.ACC[regexpr("^[0-9][0-9]{8}[V,X]",raw.data$CASA.ACC$NIC)==1,]
  print(dim(CASA.ACC))
  print(get.log.msg('Filtering valid NIC completed'))
  
  # Customer Age calculated using NIC
  print(get.log.msg('Calculating customer age started'))
  CASA.ACC$AGE <- as.numeric(strftime(Sys.Date(),"%Y"))-as.numeric(paste0("19",substr(CASA.ACC$NIC,1,2)))
  print(get.log.msg('calculating customer age completed'))
  
  # Filtering customer base AGE <= 45 and >24
  print(get.log.msg('Filtering customer base age <= 45 and 26 started'))
  CASA.ACC <- CASA.ACC[CASA.ACC$AGE <= 45,]
  CASA.ACC <- CASA.ACC[CASA.ACC$AGE > 26,]
  print(dim(CASA.ACC))
  print(get.log.msg('Filtering customer base age <= 45 and 26 completed'))
  
  # Merging CASA.ACC and Casa.tx
  print(get.log.msg('Merging CASA.ACC and Casa.tx Started'))
  raw.data$CASA.tx$NIC <- gsub(" ","",raw.data$CASA.tx$NIC)
  CASA <- merge(raw.data$CASA.tx,CASA.ACC,by ="NIC")
  print(dim(CASA))
  print(get.log.msg('Merging CASA.ACC and Casa.tx completed'))

  # Filtering customers which tx credit per month >= 25000
  print(get.log.msg('Filtering customer base Cx > 25000 (75000 for three month) started'))
  CASA <- CASA[CASA$CR_AMT >= 75000,]
  print(dim(CASA.ACC))
  print(get.log.msg('Filtering customer base Cx > 25000 (75000 for three month) completed'))  
  
  # Filtering employees
  print(get.log.msg('Filtering employees Started'))
  raw.data$Staff$NIC <- gsub(" ","",raw.data$Staff$NIC)
  CASA <- CASA [!(CASA$NIC %in% raw.data$Staff$NIC),]
  print(dim(CASA))
  print(get.log.msg('Filtering employees completed'))
    
  # Gender
  print(get.log.msg('Gender deriving started'))
  CASA <- transform(CASA,SEX=ifelse(substr(CASA$NIC,3,3)>4,"F","M"))
  print(dim(CASA))
  print(get.log.msg('Gender deriving completed'))
  
  # Merging FD information
  print(get.log.msg('Merging FD Started'))
  raw.data$FD$NIC <- gsub(" ","",raw.data$FD$NIC)
  CASAFD <- merge(CASA,raw.data$FD,by ="NIC",all.x = TRUE)
  print(dim(CASAFD))
  print(get.log.msg('Merging FD completed'))
  
  # Merging Loan informaton
  print(get.log.msg('Merging LOAN Started'))
  raw.data$LN$NIC <- gsub(" ","",raw.data$LN$NIC)
  CASAFDLN <- merge(CASAFD,raw.data$LN,by = 'NIC',all.x = TRUE)
  print(dim(CASAFDLN))
  print(get.log.msg('Merging LOAN completed'))
  
  # Merging CC information
  print(get.log.msg('Merging CC Started'))
  raw.data$CC$NIC <- gsub(" ","",raw.data$CC$NIC)
  CASAFDLNCC <- merge(CASAFDLN,raw.data$CC,by='NIC',all.x = TRUE)
  print(dim(CASAFDLNCC))
  print(get.log.msg('Merging CC completed'))
  
  # Merging Online information
  print(get.log.msg('Merging online Started'))
  raw.data$OL$NIC <- gsub(" ","",raw.data$OL$NIC)
  CASAFDLNCCOL <- merge(CASAFDLNCC,raw.data$OL,by='NIC',all.x = TRUE)
  print(dim(CASAFDLNCCOL))
  print(get.log.msg('Merging online completed'))
  
  # Merging feedback information
  print(get.log.msg('Merging feedback information Started'))
  raw.data$FB$NIC <- gsub(" ","",raw.data$FB$NIC)
  CASAFDLNCCOLFB <- merge(CASAFDLNCCOL,raw.data$FB,by='NIC',all.x = TRUE)
  print(dim(CASAFDLNCCOLFB))
  print(get.log.msg('Merging feedback information completed'))
  
  # replacing NA's with 0
  print(get.log.msg('replacing NAs with 0 Started'))
  CASAFDLNCCOLFB[is.na(CASAFDLNCCOLFB)]<-0
  print(get.log.msg('replacing NAs with 0 completed'))
  
  print(get.log.msg('Data transformation completed'))
  return(CASAFDLNCCOLFB)
  
}

engineer.new.features <- function(df){

  # ATM availablity
  print(get.log.msg('deriving ATM availability started'))
  df <- transform(df,ATM_STS=ifelse(df$DR_ATM>0,1,0))
  print(get.log.msg('deriving ATM availability completed'))
  
  # Online users
  print(get.log.msg('deriving online users started'))
  df <- transform(df,OL_STS=ifelse(df$OL_NBR>0,1,0))
  print(get.log.msg('deriving online users completed'))
  
  # Debit Card purchase users
  print(get.log.msg('deriving debit card purchase users started'))
  df <- transform(df,PH_STS=ifelse(df$PURCHASE>0,1,0))
  print(get.log.msg('deriving debit card purchase users completed'))
  
  # Standing order out users
  print(get.log.msg('deriving Standing order out users started'))
  df <- transform(df,STNO_STS=ifelse(df$DR_STN>0,1,0))
  print(get.log.msg('deriving Standing order out users completed'))
  
  # Standing order in users
  print(get.log.msg('deriving Standing order in users started'))
  df <- transform(df,STNI_STS=ifelse(df$CR_STN>0,1,0))
  print(get.log.msg('deriving Standing order in users completed'))
  
  # Standing order users
  print(get.log.msg('deriving Standing order users started'))
  df <- transform(df,STN_STS=ifelse(df$CR_STN>0 | df$DR_STN,1,0))
  print(get.log.msg('deriving Standing order users completed'))
  
  # FD users
  print(get.log.msg('deriving FD users started'))
  df <- transform(df,FD_STS=ifelse(df$FD>0,1,0))
  print(get.log.msg('deriving FD users completed'))
  
  # DR_AMT to DR_ATM ratio
  print(get.log.msg('deriving DR_AMT to DR_ATM ratio started'))
  df <- transform(df,DR2ATM=ifelse(df$DR_AMT>0,df$DR_ATM/df$DR_AMT,0))
  print(get.log.msg('deriving DR_AMT to DR_ATM ratio completed'))
  
  # Loan status
  #df <- transform(df,LN=ifelse(df$LN_ST==1 | df$LN_MT==1 | df$LN_LT==1,1,0))
  
  # CR_AMT
  print(get.log.msg('deriving CR_AMT_Log started'))
  df <- transform(df,CR_AMT_Log = ifelse(df$CR_AMT>1,log(df$CR_AMT),0))
  print(get.log.msg('deriving CR_AMT_Log completed'))
  
  # # DR_AMT
  # print(get.log.msg('deriving DR_AMT_Log started'))
  # df <- transform(df,DR_AMT_Log = ifelse(df$DR_AMT>1,log(df$DR_AMT),0))
  # print(get.log.msg('deriving DR_AMT_Log completed'))
  
  # DR_NBR
  print(get.log.msg('deriving DR_NBR_Log started'))
  df <- transform(df,DR_NBR_Log = ifelse(df$DR_NBR<1,0,log(df$DR_NBR)))
  print(get.log.msg('deriving DR_NBR_Log completed'))
  
  # CR_NBR
  print(get.log.msg('deriving CR_NBR_Log started'))
  df <- transform(df,CR_NBR_Log = ifelse(df$CR_NBR<1,0,log(df$CR_NBR)))
  print(get.log.msg('deriving CR_NBR_Log completed'))
  
  # # DR_ATM
  # print(get.log.msg('deriving DR_ATM_Log started'))
  # df <- transform(df,DR_ATM_Log = ifelse(df$DR_ATM>1,log(df$DR_ATM),0))
  # print(get.log.msg('deriving DR_ATM_Log completed'))
  
  # NBR_ATM
  print(get.log.msg('deriving NBR_ATM_Log started'))
  df <- transform(df,NBR_ATM_Log = ifelse(df$NBR_ATM<1,0,log(df$NBR_ATM)))
  print(get.log.msg('deriving NBR_ATM_Log completed'))
  
  # # Purchase
  # print(get.log.msg('deriving purchase_Log started'))
  # df <- transform(df,PURCHASE_Log = ifelse(df$PURCHASE>1,log(df$PURCHASE),0))
  # print(get.log.msg('deriving purchase_Log completed'))
  
  # #DR_STN
  # print(get.log.msg('deriving DR_STN_Log started'))
  # df <- transform(df,DR_STN_Log = ifelse(df$DR_STN>1,log(df$DR_STN),0))
  # print(get.log.msg('deriving DR_STN_Log completed'))
  
  # #CR_STN
  # print(get.log.msg('deriving CR_STN_Log started'))
  # df <- transform(df,CR_STN_Log = ifelse(df$CR_STN>1,log(df$CR_STN),0))
  # print(get.log.msg('deriving CR_STN_Log completed'))
  
  #FD
  # print(get.log.msg('deriving FD_Log started'))
  # df <- transform(df,FD_Log = ifelse(df$FD>1,log(df$FD),0))
  # print(get.log.msg('deriving FD_Log completed'))
  
  #converting the factor columns
  print(get.log.msg('converting factor column started'))
  #factor.column <- c("USR_TYP","ATM_STS","PH_STS","STNO_STS","STNI_STS","STN_STS","FD_STS","SEX","LN_DEF","LN_ST","LN_MT","LN_LT","CC_STS","FB_STS","OL_STS")
  factor.column <- c("USR_TYP","ATM_STS","PH_STS","STNO_STS","STNI_STS","STN_STS","FD_STS","SEX","LN_DEF","LN_ST","LN_MT","LN_LT","CC_STS","FB_STS","OL_STS")
  
  df[factor.column] <- lapply(df[factor.column], factor)
  print(get.log.msg('converting factor column completed'))
  return(df)
}

get.training.and.prediction.list <- function(df){
  print(get.log.msg('training and prediction list extraction started'))
  strt.date <- floor_date(Sys.Date() %m-% months(7),"month") 
  strt.date <- year(strt.date)*100+month(strt.date)
  training <- NULL
  # training data 
  # 1 : credit card active users who purchased after CASA and from feedback data
  # 0 : not interested users from feedback data
  training$data <- df[(xor(df$FB_STS ==2 , (df$CC_STS==4 & (df$CC_OPN > df$CASA_OPN) & df$CC_OPN>=strt.date)) | df$FB_STS ==1 ) ,] 
  #training$data <- df[df$FB_STS !=0 & df$FB_STS !=3,] t
  
  # Target column
  training$data  <- transform(training$data ,CC=ifelse(training$data$CC_STS==4 | training$data$FB_STS ==1 ,'Yes','No')) 
  #training$data  <- transform(training$data ,CC=ifelse(training$data$FB_STS ==1 ,'Yes','No'))
  training$data$CC <- as.factor(training$data$CC)
  
  # Outlier 
  # CR_AMT_LOG >1
  training$data <- subset(training$data, CR_AMT_Log>1)
  training$data <- training$data[c("AGE","SEX","LN_DEF","LN_ST","LN_MT","LN_LT","ATM_STS","OL_STS","PH_STS","STNO_STS","STNI_STS","STN_STS","FD_STS","DR2ATM","CR_AMT_Log","DR_NBR_Log","CR_NBR_Log","NBR_ATM_Log","CC")]
  training$features <- training$data[c("AGE","SEX","LN_DEF","LN_ST","LN_MT","LN_LT","ATM_STS","OL_STS","PH_STS","STNO_STS","STNI_STS","STN_STS","FD_STS","DR2ATM","CR_AMT_Log","DR_NBR_Log","CR_NBR_Log","NBR_ATM_Log")]

  training$PredList <- subset(df,CC_STS==0 & FB_STS==0 & CR_AMT_Log>1, select = c("NIC","AGE","SEX","LN_DEF","LN_ST","LN_MT","LN_LT","ATM_STS","OL_STS","PH_STS","STNO_STS","STNI_STS","STN_STS","FD_STS","DR2ATM","CR_AMT_Log","DR_NBR_Log","CR_NBR_Log","NBR_ATM_Log"))
  
  print(table(training$data$CC))
  print(dim(training$PredList))
  print(get.log.msg('training and prediction list extraction completed'))
  
  return(training)
}

build.the.model <- function(xgbMod){
  print(get.log.msg('model building started'))
  set.seed(123)
  xgbMod$control <- trainControl(method = "repeatedcv",
                                 number = 4,
                                 repeats = 4,
                                 classProbs = TRUE,
                                 summaryFunction = twoClassSummary,
                                 sampling = "up",
                                 allowParallel = TRUE)
  
  xgbMod$model <- train(CC ~ .-LN_DEF,
                        data=xgbMod$data,
                        method = "xgbTree",
                        trControl = xgbMod$control,
                        metric = "ROC",
                        verbose = FALSE)
  print(xgbMod$model)
  print(confusionMatrix(xgbMod$model))
  print(get.log.msg('model building completed'))
  return(xgbMod)
}
  
do.the.prediction <- function(AAMod,conn){
  print(get.log.msg('prediction Started'))
  AAMod$PredList$PRED <- predict(AAMod$model,newdata = AAMod$PredList,type = "prob")[,'Yes']
  AAMod$PredList <- AAMod$PredList[order(-AAMod$PredList$PRED),]
  print(get.log.msg('prediction completed'))
  print(get.log.msg('output saving to Hams Started'))
  sqlDrop(conn,"****",errors = FALSE)
  sqlSave(conn,AAMod$PredList[1:30000,c("NIC","AGE","SEX","PRED")],tablename = "****",colnames=FALSE)
  print(get.log.msg('output saving to Hams completed'))
  #return(AAMod)
}

write.output.to.mysql <- function(conn){
  print(get.log.msg('querying demgraphic data Started'))
  outList <- sqlQuery(conn, "****")
  outList <- unique(outList)
  print(dim(outList))
  print(get.log.msg('querying demgraphic data completed'))

write.table(outList,file="outlist.txt",fileEncoding = "utf8")
outList <- read.table(file="outlist.txt",encoding = "utf8")
file.remove("outlist.txt")

  print(get.log.msg('writing result to mysql started'))
  library("RMySQL")
  mydb<-dbConnect(MySQL(), user = '****', password='****',dbname='****', host='****')
  dbRemoveTable(mydb,"customer_modeloutput")
  dbWriteTable(mydb,name='customer_modeloutput',outList)
  dbDisconnect(mydb)
  print(get.log.msg('writing result to mysql completed'))
}

get.log.msg <- function(x){
  return(paste(Sys.time(),x))
}