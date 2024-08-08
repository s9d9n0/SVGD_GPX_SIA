
## Chargement des packages utilises dans le programme ----
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

date_jour <- Sys.time()

part_date <- as.character(Sys.Date())
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))
part_seconde <- as.character(round(second(date_jour),0))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
if (str_length(part_seconde)==1){part_seconde <- paste0("0",part_seconde)}
lib_date_jr <- paste0(part_date,"_",part_heure,"h",part_minute,"m",part_seconde,"s")
moment <- paste0(part_date,"_",part_heure,"h",part_minute)
     
rm(part_date,part_heure,part_minute,part_seconde)


##########################################################

##################################################
##                                              ##
## 1/ PARTIE REQUETE DE LA BDD CENTREON         ##
##                                              ##
################################################## ----
##
## Connexion a la nouvelle base Centreon EDIFICE Osny 100 ----
## maitre xx.xx.xx.xx ; esclave xx.xx.xx.xx

##
## Connexion a la base Centreon ----
##

# baseMdb <- dbConnect(MariaDB(), user="Gepex_lecture", password="Reporting", dbname="centreon", 
#                      host="xx.xx.xx.xx", port="3306")
# baseMdb <- dbConnect(MariaDB(), user="Gepex_lecture", password="Reporting", dbname="mysql", 
#                      host="xx.xx.xx.xx", port="3306")

baseMdb <- dbConnect(MariaDB(), user="Gepex_lecture", password="Reporting", dbname="centreon_storage", 
                     host="xx.xx.xx.xx", port="3306")


##################################################################

## REQUETE LISTING DES METRIQUES DISPONIBLES ##

# Req0 <- dbGetQuery(baseMdb, paste("SHOW TABLES FROM information_schema"))
# 
# Req0 <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLES"))
# Req0 <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLE_STATISTICS"))
# Req0 <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.FILES"))
# 
# Req0 <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLES WHERE table_schema='mysql'"))
# 
# Req0 <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLES WHERE table_schema='centreon'"))
# Req0 <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLES WHERE table_schema='centreon_storage'"))

# Req <- dbGetQuery(baseMdb, paste("SELECT *
#                                   FROM  services s
#                                   WHERE s.description LIKE 'ESX-Liste-Host' and s.enabled=1
#                                       "))

# Req <- dbGetQuery(baseMdb, paste("SELECT *
#                                   FROM  hosts h
#                                   WHERE h.name LIKE 's28esxi01.dr.sia.priv' and h.enabled=1
#                                       "))


Req <- dbGetQuery(baseMdb, paste("SELECT h.host_id, h.address, h.name AS name_host, h.instance_id AS poller,
                                         p.name AS name_poller,
                                         s.service_id, s.description AS service, s.output AS info_detail
                                  FROM             hosts h 
                                        INNER JOIN services s ON s.host_id = h.host_id
                                        INNER JOIN instances p ON h.instance_id = p.instance_id
                                  WHERE s.description LIKE 'ESX-Liste-Host' AND
                                        s.enabled=1 AND h.enabled=1
                                      "))

##################################################################

##
## Deconnexion de la base Centreon ----
##
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())
##################################################################

Req_etude <- Req

# simplification variable host_name et creation de variables comme zone...
Req_etude <- Req_etude %>%
     mutate(name_host_2=str_replace(name_host,".dc1.sia.priv$|.dc2.sia.priv$","")) %>% 
     relocate(name_host_2,.after="name_host") %>% 
     select(-name_host) %>% rename(name_host=name_host_2)

Req_etude <- Req_etude %>%
     mutate(lg=row_number(),
            longueur=str_length(info_detail),
            zone = case_when(
               name_poller %in% c("Pollerdc200","Poller1dc200")  ~ "01_DMZ_dc2",
               name_poller %in% c("Pollerdc100","Poller1dc100")  ~ "02_DMZ_dc1",
               name_poller %in% c("Pollerdc225")                 ~ "03_Part_dc2",
               name_poller %in% c("Pollerdc125")                 ~ "04_Part_dc1",
               name_poller %in% c("Pollerdc250","Poller1dc250",
                                  "Poller2dc250","Poller3dc250") ~ "05_ZoneInt_dc2",
               name_poller %in% c("Pollerdc150","Poller1dc150",
                                  "Poller2dc150","Rsdc150")      ~ "06_ZoneInt_dc1",
               name_poller %in% c("Rsdc2100","Pollerdc2100")     ~ "07_ZoneSIA_dc2",
               name_poller %in% c("Central","Pollerdc1100")      ~ "08_ZoneSIA_dc1",
          TRUE ~ "09_Autre")) %>% 
     relocate(zone,.after=name_poller)

# traitement du contenu de la variable info_detail pour obtenir 1 VM par ligne 
Req_transit <- Req_etude %>% select(lg, info_detail) %>% separate_rows(info_detail,sep="].n") %>% 
     separate(info_detail,sep="] :.n", into=c("ESXI","VM")) %>% 
     mutate(ESXI_Hote=ifelse(str_detect(ESXI,"List ESX"),ESXI,""),
            VM=ifelse(is.na(VM),ESXI,VM)) %>% 
     select(-ESXI) %>% relocate(ESXI_Hote,.after="lg") %>% 
     mutate(VM=str_trim(VM)) %>% filter(VM!="") %>% 
     mutate(posPowered_start=str_locate(VM,"powered")[,1],
            posPowered_end=str_locate(VM,"powered")[,2]) %>% 
     mutate(etatVM=str_sub(VM,posPowered_end+1,str_length(VM)),
            VM=str_sub(VM,1,posPowered_start-2)) %>% 
     select(-ESXI_Hote,-posPowered_start,-posPowered_end)

# jointure
Req_etude <- Req_etude %>% left_join(Req_transit,by=c("lg")) %>% 
     select(-info_detail,-lg,-longueur) %>% rename(Esxi=name_host) %>% 
     mutate(moment=moment) %>% relocate(moment)

Agreg <- Req_etude %>% group_by(Esxi,etatVM) %>% 
     summarise(eff=n()) %>% ungroup() %>% as.data.frame() %>% 
     pivot_wider(names_from = etatVM, values_from = eff) %>% 
     select(Esxi,On,Off) %>% 
     rename(etatOn=On,etatOff=Off) %>% 
     mutate(etatOn=ifelse(is.na(etatOn),0,etatOn),
            etatOff=ifelse(is.na(etatOff),0,etatOff))
Agreg2 <- Agreg %>% summarise(nbOn=sum(etatOn),nbOff=sum(etatOff))


Req_VM <- Req_etude %>% select(moment, Esxi, VM, etatVM)
     

########################################################
########################################################
# sauvegarde des resultats dans un repertoire Windows

# Creation dun repertoire avec la date du jour
folder <- paste0("C:/Users/siar_ycg8l6/ProgR/HistoVMW_EsxListeHost/Fichiers_date_",format(Sys.time(),'%Y%m%d'))
if (file.exists(folder)) {
     cat("The folder already exists")
} else {
     dir.create(folder, showWarnings = TRUE, recursive = FALSE, mode = "0777")
}

write.csv2(Req_VM, 
           paste0(folder,"/List_EsxiVM_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)








