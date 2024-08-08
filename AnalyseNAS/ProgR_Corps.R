
# Chargement des packages utiles pour le programme
library(lubridate)
library(RMariaDB)
library(dplyr)
library(tidyr)
library(stringr)
library(readODS)

rep_gen <- "C:/Users/siar_ycg8l6/ProgR/Analyse_Nas/"

# decalage egal a 2 en heure d'?t? et ? 1 en heure d'hiver
decalage_heure <- 2

##################################################################

# date_jour <- Sys.time()

# "01","02","03","04","05","06","07"
# "08","09","10","11","12","13","14"
# "15","16","17","18","19","20","21"
# "22","23","24","25","26","27","28"

vect_jour <- as.character(Sys.Date())
#vect_jour <- c("20")

vect <- vect_jour %>% as.data.frame() %>% rename(listejour=".") %>% 
     mutate(listejour=paste0(listejour," 00:00:00"))     
     #mutate(listejour=paste0("2024-05-",listejour," 00:00:00"))

vect_jour <- c()
for(i in 1:nrow(vect)){
     vect_jour <- append(vect_jour,vect$listejour[i])
}
rm(i,vect)
vect_jour

# for (date in vect_jour){
     date_jour_ref0 <- as.POSIXct(vect_jour)
     # date_jour_ref0 <- as.POSIXct(date)
     date_jour_ref1 <- date_jour_ref0 + ddays(1)
     
     date_jour_ref0 <- date_jour_ref0 - dhours(decalage_heure)
     date_jour_ref1 <- date_jour_ref1 - dhours(decalage_heure)
     
     partie_jour <- as.character(day(date_jour_ref1))
     partie_mois <- as.character(month(date_jour_ref1))
     partie_an <- str_sub(as.character(year(date_jour_ref1)),3,4)
     
     if (str_length(partie_jour)==1){
          partie_jour <- paste0("0",partie_jour)
     }
     if (str_length(partie_mois)==1){
          partie_mois <- paste0("0",partie_mois)
     }
     
     
     type <- "data"
     source(paste0(rep_gen,"ProgR_Recup_Quotas_data.R"))
     cat(paste0("partie data, jour : ",partie_jour,"/",partie_mois,"/",partie_an," termin?\n"))
     
     
     type <- "logs"
     source(paste0(rep_gen,"ProgR_Recup_Quotas_logs.R"))
     cat(paste0("partie logs, jour : ",partie_jour,"/",partie_mois,"/",partie_an," termin?\n"))
     
     
     source(paste0(rep_gen,"ProgR_Recup_Volumes.R"))
     cat(paste0("partie volumes, jour : ",partie_jour,"/",partie_mois,"/",partie_an," termin?\n"))
# }


