
# Chargement des packages utiles pour le programme
library(lubridate)
library(RMariaDB)
library(dplyr)
library(tidyr)
library(stringr)
library(readODS)

rep_gen <- "C:/Users/siar_ycg8l6/ProgR/Analyse_Nas/"

# decalage egal a 2 en heure d'ete et a 1 en heure d'hiver
decalage_heure <- 2

##################################################################

# date_jour <- Sys.time()

# "01","02","03","04","05","06","07"
# "08","09","10","11","12","13","14"
# "15","16","17","18","19","20","21"
# "22","23","24","25","26","27","28"
vect_jour <- c("06","07")

vect <- vect_jour %>% as.data.frame() %>% rename(listejour=".") %>% 
     mutate(listejour=paste0("2023-07-",listejour," 00:00:00"))
vect_jour <- c()
for(i in 1:nrow(vect)){
     vect_jour <- append(vect_jour,vect$listejour[i])
}
rm(i,vect)
vect_jour

for (date in vect_jour){
     date_jour_ref0 <- as.POSIXct(date)
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
}


##################################################################################
##################################################################################

# PARTIE REQUETE DE LA BDD CENTREON

# Connexion base DC1 100% 
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon_storage", host="XX.XX.XX.XX", port="3306")

# Connexion base DC2 100% 
# baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
#                      dbname="centreon_storage", host="XX.XX.XX.XX", port="3306")
# pas de flux access denied

##################################################################################

# recuperation des datas sur les 24h de la journee consideree
Req_Logs <- dbGetQuery(baseMdb,
                       paste("SELECT i.host_id, i.host_name, m.metric_name,
                                v.ctime, v.value, m.unit_name
                         FROM data_bin v
                              INNER JOIN metrics m on v.id_metric = m.metric_id
                              INNER JOIN index_data i on m.index_id = i.id
                         WHERE i.host_name LIKE ('%pd-nas1%') AND
                               i.service_description LIKE ('%Quotas_Data%') AND
                               v.ctime >= UNIX_TIMESTAMP('",date_jour_ref0,"') AND
                               v.ctime <= UNIX_TIMESTAMP('",date_jour_ref1,"')
                         ORDER BY v.ctime DESC
                        "))

Req_Logs <- dbGetQuery(baseMdb,
                       paste("SELECT l.ctime, l.host_name, l.service_description, l.output
                         FROM logs l
                         WHERE l.host_name LIKE ('%pd-nas1%') AND
                               l.service_description LIKE ('%Quotas_Data%') AND
                               l.ctime >= UNIX_TIMESTAMP('",date_jour_ref0,"') AND
                               l.ctime <= UNIX_TIMESTAMP('",date_jour_ref1,"')
                         ORDER BY l.ctime DESC
                        "))


Req_Logs <- dbGetQuery(baseMdb,
                       paste("SELECT l.ctime, l.instance_name, l.host_name, l.service_description, l.output
                         FROM logs l
                         WHERE #l.host_name LIKE ('%pd-nas1%') AND
                               l.ctime >= UNIX_TIMESTAMP('",date_jour_ref0,"') AND
                               l.ctime <= UNIX_TIMESTAMP('",date_jour_ref1,"')
                         ORDER BY l.ctime DESC
                        "))

# Deconnexion
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())

##################################################################################
##################################################################################

Req_Agreg <- Req_Logs %>% group_by(instance_name) %>% 
     summarize(nobs=n()) %>% ungroup() %>% as.data.frame()


