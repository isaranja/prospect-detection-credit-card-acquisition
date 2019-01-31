args<-commandArgs(TRUE)

setwd("R")

logFile <- file("R_log.txt", open = "at")
sink(logFile)
sink(logFile, type = "message",append = TRUE)

# loading the required functions
source('functionList_mod_live.R')

print(get.log.msg('new batch execution'))
print('')
print('********************************')

library(PKI)
library(RODBC)
library(magrittr)
library(caret)
library(lubridate)
library(RMySQL)

host <- args[1];
db <- args[2];
userName <- args[3];

key <- PKI.digest("*********", "MD5");
cypherText <- base64decode(args[4]);
 
print(cypherText)
db_password <- PKI.decrypt(cypherText, key, "********")
db_password <- rawToChar(db_password)

print(get.log.msg('RODBC connection opened'))

conn <- odbcDriverConnect(paste0('driver={iSeries Access ODBC Driver};System=', '*****', ';Uid=', userName, ';Pwd=', db_password));

print(conn)

WIP  <- get.raw.data(conn) %>% transformData() %>% engineer.new.features()

get.training.and.prediction.list(WIP) %>% build.the.model() %>% do.the.prediction(.,conn)

write.output.to.mysql(conn) # to change. this is used since UAT library is difference

odbcClose(conn)

sink(type = "message")
sink()