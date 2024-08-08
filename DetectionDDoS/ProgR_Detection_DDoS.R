
## Chargement des differents packages utilises dans le programme ----
library(dplyr)
library(stringr)
library(tidyr)
library(readr)
library(RMariaDB)
library(lubridate)

rep_gen <- "C:/Users/siar_ycg8l6/ProgR/Detection_DDoS/"

# rep_sas <- "E:/X_Passerelle_vers_Z050/"
rep_sas <- "C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/"

# decalage egal de 2 en heure en ete et 1 en heure en hiver
decalage_heure <- 2

# Vraies valeurs
seuilWarn_PaloAlto_Osny <- 150000
seuilCrit_PaloAlto_Osny <- 200000

seuilWarn_PaloAlto_Auze <- 125000
seuilCrit_PaloAlto_Auze <- 175000

# valeurs test
# seuilWarn_PaloAlto_Osny <- 50000
# seuilCrit_PaloAlto_Osny <- 60000
# 
# seuilWarn_PaloAlto_Auze <- 80000
# seuilCrit_PaloAlto_Auze <- 85000


###

# Vraies valeurs
seuilWarn_DNS_Osny <- 25000
seuilCrit_DNS_Osny <- 35000

seuilWarn_DNS_Auze <- 20000
seuilCrit_DNS_Auze <- 30000

# valeurs test
# seuilWarn_DNS_Osny <- 2500
# seuilCrit_DNS_Osny <- 3500
# 
# seuilWarn_DNS_Auze <- 1000
# seuilCrit_DNS_Auze <- 2000

##################################################################

date_jour <- Sys.time()

part_date <- as.character(Sys.Date())
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))
part_seconde <- as.character(round(second(date_jour),0))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
if (str_length(part_seconde)==1){part_seconde <- paste0("0",part_seconde)}
lib_date_jr <- paste0(part_date,"_",part_heure,"h",part_minute,"m",part_seconde,"s")

rm(part_date,part_heure,part_minute,part_seconde)


date_jour_ref1 <- date_jour - dhours(decalage_heure)
date_jour_ref0 <- date_jour - dhours(decalage_heure) - dminutes(15)

##################################################################################
##################################################################################

# PARTIE REQUETE DE LA BDD CENTREON

# Connexion base Osny 100% 
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon_storage", host="xx.xx.xx.XX", port="3306")

##################################################################################

# recuperation des datas sur les Pare-Feux DMZ PaloAlto
Req_fw01 <- dbGetQuery(baseMdb,
                       paste("SELECT i.host_name, m.metric_name, v.ctime, v.value
                         FROM data_bin v
                              INNER JOIN metrics m on v.id_metric = m.metric_id
                              INNER JOIN index_data i on m.index_id = i.id
                         WHERE i.host_name LIKE ('%pd-fw01%') AND
                               i.service_description LIKE ('%Sessions%') AND
                               m.metric_name IN ('sessions.active.udp.count',
                                                 'sessions.active.tcp.count') AND
                               v.ctime >= UNIX_TIMESTAMP('",date_jour_ref0,"') AND
                               v.ctime <= UNIX_TIMESTAMP('",date_jour_ref1,"')
                         ORDER BY v.ctime DESC
                        "))

# recuperation des datas sur les EfficientIP
Req_EffIP <- dbGetQuery(baseMdb,
                       paste("SELECT i.host_name, m.metric_name, v.ctime, v.value
                         FROM data_bin v
                              INNER JOIN metrics m on v.id_metric = m.metric_id
                              INNER JOIN index_data i on m.index_id = i.id
                         WHERE (i.host_name LIKE ('%pd-ddi01-id16%') OR
                                i.host_name LIKE ('%pd-ddi01-id26%')) AND
                               i.service_description LIKE ('%Dns-General%') AND
                               m.metric_name IN ('response','tcp','udp') AND
                               v.ctime >= UNIX_TIMESTAMP('",date_jour_ref0,"') AND
                               v.ctime <= UNIX_TIMESTAMP('",date_jour_ref1,"')
                         ORDER BY v.ctime DESC
                        "))

# Deconnexion
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())

##################################################################################
##################################################################################

Requete <- Req_fw01 %>% 
     mutate(POSIXct=ymd_hms(as_datetime(ctime))+dhours(decalage_heure)) %>% 
     select(-ctime) %>% 
     arrange(host_name,POSIXct) %>% 
     pivot_wider(names_from = metric_name, values_from = value) %>% as.data.frame() %>%
     rename(sessions_udp = sessions.active.udp.count,
            sessions_tcp = sessions.active.tcp.count) %>% 
     relocate(sessions_tcp,.before=sessions_udp) %>% 
     mutate(lg=row_number()) %>% relocate(lg) %>% 
     mutate(sessions_tot = sessions_udp + sessions_tcp) %>% 
     mutate(seuil = case_when(
               host_name %in% c("pd-fw01-id161","pd-fw01-id162") & 
                    sessions_tot>seuilCrit_PaloAlto_Osny ~ "Crit",
               host_name %in% c("pd-fw01-id161","pd-fw01-id162") & 
                    sessions_tot>seuilWarn_PaloAlto_Osny ~ "Warn",
               
               host_name %in% c("pd-fw01-id261","pd-fw01-id262") & 
                    sessions_tot>seuilCrit_PaloAlto_Auze ~ "Crit",
               host_name %in% c("pd-fw01-id261","pd-fw01-id262") & 
                    sessions_tot>seuilWarn_PaloAlto_Auze ~ "Warn",
               TRUE ~ "")
            )

Requete <- Requete %>% 
     mutate(last=ifelse(host_name!=lead(host_name) | lg==max(lg),1,0)) %>% 
     select(-lg) %>% 
     mutate(diff_tcp=ifelse(lag(host_name)==host_name,sessions_tcp-lag(sessions_tcp),NA),
            diff_udp=ifelse(lag(host_name)==host_name,sessions_udp-lag(sessions_udp),NA),
            diff_tot=ifelse(lag(host_name)==host_name,sessions_tot-lag(sessions_tot),NA))

indic_Alerte <- Requete %>% 
     filter( (last==1 & seuil %in% c("Warn","Crit")) |
             (last==1 & (abs(diff_tcp)>20000 | abs(diff_udp)>20000 | abs(diff_tot)>20000)) ) %>% 
     dim.data.frame()


###


Requete2 <- Req_EffIP %>% 
     mutate(host_name_2=str_replace(host_name,".dc1.sia.priv$|.dc2.sia.priv$","")) %>% 
     relocate(host_name_2,.after="host_name") %>% 
     select(-host_name) %>% rename(host_name=host_name_2) %>% 
     mutate(POSIXct=ymd_hms(as_datetime(ctime))+dhours(decalage_heure)) %>% 
     select(-ctime) %>% 
     arrange(host_name,POSIXct) %>% 
     pivot_wider(names_from = metric_name, values_from = value) %>% as.data.frame() %>%
     relocate(tcp,.before=response) %>% 
     relocate(udp,.before=response) %>% 
     mutate(lg=row_number()) %>% relocate(lg) %>% 
     mutate(seuil = case_when(
          str_detect(host_name,"pd-ddi01-id16") & 
               response>seuilCrit_DNS_Osny  ~ "Crit",
          str_detect(host_name,"pd-ddi01-id16") & 
               response>seuilWarn_DNS_Osny ~ "Warn",
          
          str_detect(host_name,"pd-ddi01-id26") & 
               response>seuilCrit_DNS_Auze ~ "Crit",
          str_detect(host_name,"pd-ddi01-id26") & 
               response>seuilWarn_DNS_Auze ~ "Warn",
          TRUE ~ "")
     )

Requete2 <- Requete2 %>% 
     mutate(last=ifelse(host_name!=lead(host_name) | lg==max(lg),1,0)) %>% 
     select(-lg) %>% 
     mutate(diff_tcp=ifelse(lag(host_name)==host_name,tcp-lag(tcp),NA),
            diff_udp=ifelse(lag(host_name)==host_name,udp-lag(udp),NA),
            diff_resp=ifelse(lag(host_name)==host_name,response-lag(response),NA))

indic_Alerte2 <- Requete2 %>% 
     filter( (last==1 & seuil %in% c("Warn","Crit")) |
             (last==1 & (abs(diff_tcp)>20000 | abs(diff_udp)>20000 | abs(diff_resp)>20000)) ) %>% 
     dim.data.frame()


gc(reset=TRUE)


###


if (indic_Alerte[1]>=1 | indic_Alerte2[1]>=1){
     # enregistrement dans un fichier
     write.csv2(Requete,  paste0(rep_gen,"df_PaloAlto_",lib_date_jr,".csv"), row.names=FALSE)
     write.csv2(Requete2, paste0(rep_gen,"df_Dns_",lib_date_jr,".csv"),      row.names=FALSE)
     # copie de ce fichier vers le repertoire sas
     write.csv2(Requete,  paste0(rep_sas,"df_PaloAlto_",lib_date_jr,".csv"), row.names=FALSE)
     write.csv2(Requete2, paste0(rep_sas,"df_Dns_",lib_date_jr,".csv"),      row.names=FALSE)
     
} 






