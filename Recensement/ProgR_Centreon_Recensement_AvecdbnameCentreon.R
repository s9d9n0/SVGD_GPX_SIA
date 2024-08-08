
#.libPaths()

# chemin ou se trouve les packages R (retrouvable avec la commande .libPath() :
# "C:/Program Files/R/R-4.2.1/library" 
# "C:/Users/R_ycg8l6/AppData/Local/R/win-library/4.2"

library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)


##################################################
##                                              ##
## 1/ PARTIE REQUETE DE LA BDD CENTREON         ##
##                                              ##
################################################## ----
##
## Connexion a la nouvelle base Centreon EDIFICE Osny 100 ----
## maitre XX.XX.XX.XX ; esclave XX.XX.XX.XX
##
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon", host="XX.XX.XX.XX", port="3306")

# !! utilisation de la base centreon (tout court) et non centreon.storage

##################################################################

# Req <- dbGetQuery(baseMdb, paste("SHOW TABLES FROM information_schema"))
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLES"))
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM information_schema.TABLES WHERE table_schema='centreon'"))

# 
# ReqA <- dbGetQuery(baseMdb, paste("SELECT * FROM contact_password"))
# ReqA <- dbGetQuery(baseMdb, paste("SELECT * FROM acl_resources"))
# 
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM host"))
# 
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM nagios_server"))
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM ns_host_relation"))
# 
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM service"))
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM host_service_relation"))
# 
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM hostgroup"))
# Req <- dbGetQuery(baseMdb, paste("SELECT * FROM hostgroup_relation"))
# 
# var_df <- colnames(Req) %>% as.data.frame()

Req_HS <- dbGetQuery(baseMdb,
                      paste("SELECT p.id as poller_id, p.name as name_poller,
                                    h.host_id, h.host_name as name_host, h.host_address, h.host_activate as active_host,
                                    s.service_id, s.service_activate
                             FROM           nagios_server p
                                  LEFT JOIN ns_host_relation hp on hp.nagios_server_id = p.id
                                  LEFT JOIN host h on h.host_id = hp.host_host_id
                                  LEFT JOIN host_service_relation hs on hs.host_host_id = h.host_id
                                  LEFT JOIN service s on s.service_id = hs.service_service_id
                             WHERE host_activate='1' and service_activate='1'
                            "))

Req_GH <- dbGetQuery(baseMdb, 
                     paste("SELECT hg.hg_id as hostgroup_id, hg.hg_name as name_hostgroup,
                                   h.host_id, h.host_name as name_host
                            FROM           host h 
                                 LEFT JOIN hostgroup_relation hhg on h.host_id = hhg.host_host_id
                                 LEFT JOIN hostgroup hg on hhg.hostgroup_hg_id = hg.hg_id
                           "))

##################################################################

##
## Deconnexion de la base Centreon ----
##
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())
##################################################################

#####

##
## Connexion a la nouvelle base Centreon EDIFICE Osny 100 ----
## maitre XX.XX.XX.XX ; esclave XX.XX.XX.XX
##
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon_storage", host="XX.XX.XX.XX", port="3306")

# !! utilisation de la base centreon_storage ici pour recuperer le niveau metrique

##################################################################

Req_SM <- dbGetQuery(baseMdb,
                    paste("SELECT s.service_id, s.description, s.enabled as active_service,
                                  s.check_command, s.command_line, s.check_period, s.check_interval,
                                  i.id AS i_id, i.service_id as service_id_index,
                                  m.index_id AS m_id, m.metric_name, m.unit_name, m.warn, m.crit
                           FROM           services s
                                LEFT JOIN index_data i on i.service_id = s.service_id
                                LEFT JOIN metrics m on m.index_id = i.id
                          "))

##################################################################

##
## Deconnexion de la base Centreon ----
##
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())
##################################################################

#####

Req_HS_2 <- Req_HS %>% 
     mutate(name_host_2=str_replace(name_host,
          ".insee.fr$|.dmz.insee$|.part.insee$|.insee.intra$|.ad.insee.intra$|.insee.fr$|.adsia.sia.priv$",
          "")) %>% 
     relocate(name_host_2,.after="name_host") %>% 
     select(-name_host) %>% rename(name_host=name_host_2)

# fusion entre les df Hote-Service et Service-Metrique
Req_HSM <- Req_HS_2 %>% 
     left_join(Req_SM, by=c("service_id")) %>% select(-i_id,-service_id_index)



Req_GH_2 <- Req_GH %>% 
     mutate(name_host_2=str_replace(name_host,
          ".insee.fr$|.dmz.insee$|.part.insee$|.insee.intra$|.ad.insee.intra$|.insee.fr$|.adsia.sia.priv$",
          "")) %>% 
     relocate(name_host_2,.after="name_host") %>% 
     select(-name_host) %>% rename(name_host=name_host_2)

List_Host_id <- Req_HS %>% select(host_id) %>% unique()

Req_GH_2 <- Req_GH_2 %>% right_join(List_Host_id, by=c("host_id")) 
# un peu plus d'obs que le df List_Host_id car des hotes peuvent avoir un rattachement
# a plusieurs groupes d'hotes


## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

Vue1 <- Req_GH_2 %>% group_by(name_hostgroup,name_host) %>% 
     summarize(eff=n()) %>% ungroup() %>% as.data.frame() %>% 
     # et quelques modifications ponctuelles...
     mutate(name_hostgroup=str_replace(name_hostgroup,"^Vlan","-Vlan")) %>% 
     mutate(name_hostgroup=str_replace(name_hostgroup,"_DA_DirectAccess$","_DirectAccess")) %>% 
     mutate(name_hostgroup=str_replace(name_hostgroup,"DC1_100_PD_Supervision","DC1_100_PD_Superv")) %>%
     mutate(name_hostgroup=str_replace(name_hostgroup,"^-Etab","--Etab")) %>%     
     arrange(name_hostgroup)

Vue2 <- pivot_wider(Vue1, names_from=name_hostgroup, values_from = name_hostgroup) %>%
     as.data.frame() %>% arrange(name_host)

variable_first <- colnames(Vue2)[3]
variable_last  <- colnames(Vue2)[ncol(Vue2)]

Vue3 <- unite(Vue2,
              variable_first:variable_last, 
              col="Gr_Hotes", sep=" // ", na.rm = TRUE, remove = TRUE) %>% 
     select(name_host,Gr_Hotes) %>% arrange(name_host) %>% 
     # et quelques modifications ponctuelles...
     mutate(Gr_Hotes=str_replace(Gr_Hotes,"-DirectAccess // -Vlan_DC1_00_PD_DirectAccess","-Vlan_DC1_00_PD_DirectAccess")) %>% 
     mutate(Gr_Hotes=str_replace(Gr_Hotes,"^-RepartiteursCharge // ","")) %>% 
     mutate(Gr_Hotes=str_replace(Gr_Hotes," // -Switchs-Coeurs_Etab // -Switchs-INSEE"," // -Switchs-Coeurs")) %>% 
     mutate(Gr_Hotes=str_replace(Gr_Hotes," // -Switchs-Etage_Etab // -Switchs-INSEE"," // -Switchs-Etage")) %>% 
     mutate(Gr_Hotes=str_replace(Gr_Hotes,"--Etab","-Etab"))

Vue4 <- Vue3 %>% filter(str_detect(Gr_Hotes,"//"))

# liste des hotes sans rattachement a aucun groupe d'hotes
List_Host_id_2 <- Req_HS_2 %>% select(host_id,name_host) %>% unique()

Req_GH_3 <- List_Host_id_2 %>% left_join(Req_GH_2, by=c("host_id")) %>% select(-name_host.y) %>% 
     rename(name_host=name_host.x) %>% filter(is.na(name_hostgroup))
# on trouve 1 hotes
# Services_Ressources_Externes_INSEE = census.insee.net

## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

# fusion entre les df Hote-Service-Metrique et Groupe Hote-Hote
Req_HGHSM <- Req_HSM %>% 
     left_join(Vue3, by=c("name_host")) %>% relocate(Gr_Hotes,.after="host_address")

Req_HGHSM <- Req_HGHSM %>%
     mutate(zone = case_when(
          name_poller %in% c("PollerAuzeville00","Poller1Auzeville00")  ~ "01_DMZ_Auzeville",
          name_poller %in% c("PollerOsny00","Poller1Osny00")            ~ "02_DMZ_Osny",
          name_poller %in% c("PollerAuzeville25")                       ~ "03_Partenaire_Auzeville",
          name_poller %in% c("PollerOsny25")                            ~ "04_Partenaire_Osny",
          name_poller %in% c("PollerAuzeville50","Poller1Auzeville50",
                             "Poller2Auzeville50","Poller3Auzeville50") ~ "05_ZoneInterne_Auzeville",
          name_poller %in% c("PollerOsny50","Poller1Osny50",
                             "Poller2Osny50","RsOsny50")                ~ "06_ZoneInterne_Osny",
          name_poller %in% c("RsAuzeville100","PollerAuzeville100")     ~ "07_ZoneSIA_Auzeville",
          name_poller %in% c("Central","PollerOsny100")                 ~ "08_ZoneSIA_Osny",
          TRUE ~ "07_Autre")) %>% 
     relocate(zone,.after=name_poller) %>% 
     mutate(nbcontr_jr = 1440 / check_interval) %>% relocate(nbcontr_jr,.after=check_interval)

# il y a quelques controles pour lesquels le check_interval n'est pas renseigne, 
# on les retire car peu nombreux
visu <- Req_HGHSM %>% filter(is.na(nbcontr_jr))

Vue_ZoneControle <- Req_HGHSM %>% filter(!is.na(nbcontr_jr)) %>% group_by(zone) %>% 
     summarise(nb_obs=sum(nbcontr_jr)) %>%  ungroup() %>% as.data.frame() %>% rename(nb_contr=nb_obs)

Vue_ZoneMetrique <- Req_HGHSM %>% group_by(zone) %>% 
     summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% rename(nb_metr=nb_obs)

Vue_ZoneService <- Req_HGHSM %>% group_by(zone,service_id) %>% 
     summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1) %>% 
     group_by(zone) %>% 
     summarise(nb_obs=sum(nb_obs)) %>%  ungroup() %>% as.data.frame() %>% rename(nb_serv=nb_obs)

Vue_ZoneHote <- Req_HGHSM %>% group_by(zone,host_id) %>% 
     summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1) %>% 
     group_by(zone) %>% 
     summarise(nb_obs=sum(nb_obs)) %>%  ungroup() %>% as.data.frame() %>% rename(nb_hote=nb_obs)

visu_2 <- Req_HGHSM %>% filter(zone=="07_Autre")

VueGlobale <- Vue_ZoneHote %>%
     left_join(Vue_ZoneService, by=c("zone")) %>% 
     left_join(Vue_ZoneMetrique, by=c("zone")) %>%
     left_join(Vue_ZoneControle, by=c("zone"))

write.csv2(VueGlobale,"C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/VueGlobale_AvecSchemaCentreon.csv",row.names=FALSE)

rm(Vue_ZoneHote,Vue_ZoneService,Vue_ZoneMetrique,Vue_ZoneControle)


##################################################################################################
##################################################################################################

####
####
# FOCUS SUR zone Auzevile 100
####
####

# Vue_ZoneControle_100Auze <- Req_HGHSM %>% filter(zone=="07_ZoneSIA_Auzeville") %>%
#      group_by(name_host) %>% 
#      summarise(nb_obs=sum(nbcontr_jr)) %>%  ungroup() %>% as.data.frame() %>% rename(nb_contr=nb_obs)
# 
# Vue_ZoneMetrique_100Auze <- Req_HGHSM %>% filter(zone=="07_ZoneSIA_Auzeville") %>% 
#      group_by(name_host) %>% 
#      summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% rename(nb_metr=nb_obs)
# 
# Vue_ZoneService_100Auze <- Req_HGHSM %>% filter(zone=="07_ZoneSIA_Auzeville") %>%
#      group_by(name_host,service_id) %>% 
#      summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1) %>% 
#      group_by(name_host) %>% 
#      summarise(nb_obs=sum(nb_obs)) %>%  ungroup() %>% as.data.frame() %>% rename(nb_serv=nb_obs)
# 
# Vue_ZoneHote_100Auze <- Req_HGHSM %>% filter(zone=="07_ZoneSIA_Auzeville") %>%
#      group_by(name_host,host_id) %>% 
#      summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1) %>% 
#      group_by(name_host) %>% 
#      summarise(nb_obs=sum(nb_obs)) %>%  ungroup() %>% as.data.frame() %>% rename(nb_hote=nb_obs)
# 
# 
# VueGlobale_100Auze <- Vue_ZoneHote_100Auze %>%
#      left_join(Vue_ZoneService_100Auze, by=c("name_host")) %>% 
#      left_join(Vue_ZoneMetrique_100Auze, by=c("name_host")) %>%
#      left_join(Vue_ZoneControle_100Auze, by=c("name_host"))
# 
# write.csv2(VueGlobale_100Auze,
#            "C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/VueGlobale_AvecSchemaCentreon_100Auze.csv",
#            row.names=FALSE)


# Vision particuliere sur un hote Auzeville
# VueZoom <- Req_HGHSM %>% 
#      filter(zone=="07_ZoneSIA_Auzeville" & name_host=="pd-vmw-m1-id205.dc2.sia.priv") %>% 
#      group_by(description) %>% summarise(nbobs=n()) %>% ungroup() %>% as.data.frame()
# 
# VueZoom <- Req_HGHSM %>% 
#      filter(zone=="07_ZoneSIA_Auzeville" & name_host=="pd-vmw-m1-id206.dc2.sia.priv") %>% 
#      group_by(description) %>% summarise(nbobs=n()) %>% ungroup() %>% as.data.frame()
# 
# 
# VueZoom <- Req_HGHSM %>% 
#      filter(zone=="07_ZoneSIA_Auzeville" & str_detect(name_host,"pdvcsa0d2ast91")) %>% 
#      group_by(description) %>% summarise(nbobs=n()) %>% ungroup() %>% as.data.frame()
# 
# VueZoom <- Req_HGHSM %>% 
#      filter(zone=="07_ZoneSIA_Auzeville" & str_detect(name_host,"pdvcsa6d2ast91")) %>% 
#      group_by(description) %>% summarise(nbobs=n()) %>% ungroup() %>% as.data.frame()

####
####
# DEMANDE DU 11 JUIL DE LUDOVIC LISTE VM SUR ZONE 100
####
####

# Visu_ZoneVM_100Auze <- Req_HGHSM %>% filter(zone=="07_ZoneSIA_Auzeville") %>%
#      group_by(name_host) %>% 
#      summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame()
# 
# Visu_ZoneVM_100Osny <- Req_HGHSM %>% filter(zone=="08_ZoneSIA_Osny") %>%
#      group_by(name_host) %>% 
#      summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame()
# 
# write.csv2(Visu_ZoneVM_100Auze,
#            "C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/Visu_ZoneVM_100Auze.csv",
#            row.names=FALSE)
# write.csv2(Visu_ZoneVM_100Osny,
#            "C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/Visu_ZoneVM_100Osny.csv",
#            row.names=FALSE)

####
####
# POUR PRES VM DATASCIENCE d'AURELIEN
####
####

# datsci_050Osny <- Req_HGHSM %>% filter(zone=="06_ZoneInterne_Osny" & str_detect(name_host,"datsci"))
# datsci_050Auze <- Req_HGHSM %>% filter(zone=="05_ZoneInterne_Auzeville" & str_detect(name_host,"datsci"))
#      
# datsci <- rbind(datsci_050Osny,datsci_050Auze)
# 
# write.csv2(datsci,
#            "C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/ListingCentreon_datascience.csv",
#            row.names=FALSE)

####
####
# POUR VISU DOUBLON POTENTIEL DMZ
####
####

# Vue <- Req_GH %>% filter(str_detect(name_host,"insee.fr|dmz.insee")) %>% 
#      mutate(name_host_2=str_replace(name_host,".insee.fr$|.dmz.insee$","")) %>% 
#      arrange(name_host_2)
# write.csv2(Vue,
#           "C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/ListingCentreon_DMZ_doublon.csv",
#           row.names=FALSE)


##################################################################################################
##################################################################################################

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

########################################################
########################################################
# sauvegarde des resultats dans un repertoire Windows

# Creation dun repertoire avec la date du jour
folder <- paste0("C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/Fichiers_date_",format(Sys.time(),'%Y%m%d'))
if (file.exists(folder)) {
     cat("The folder already exists")
} else {
     dir.create(folder, showWarnings = TRUE, recursive = FALSE, mode = "0777")
}

# DEBUT DES EXPORTS CSV (Apres traitements) ....

Vue_Auzeville <- VueGlobale %>% filter(str_detect(zone,"Auzeville")) %>%
     mutate(allvar="09_Ensemble Auzeville") %>% relocate(allvar) %>% group_by(allvar) %>% 
     summarise(nb_hote=sum(nb_hote),nb_serv=sum(nb_serv),nb_metr=sum(nb_metr),nb_contr=sum(nb_contr)) %>%
     ungroup() %>% as.data.frame() %>% rename(zone=allvar)
Vue_Osny <- VueGlobale %>% filter(str_detect(zone,"Osny")) %>%
     mutate(allvar="10_Ensemble Osny") %>% relocate(allvar) %>% group_by(allvar) %>% 
     summarise(nb_hote=sum(nb_hote),nb_serv=sum(nb_serv),nb_metr=sum(nb_metr),nb_contr=sum(nb_contr)) %>%
     ungroup() %>% as.data.frame() %>% rename(zone=allvar)
Vue_Total <- VueGlobale %>%
     mutate(allvar="11_Ensemble SI") %>% relocate(allvar) %>% group_by(allvar) %>% 
     summarise(nb_hote=sum(nb_hote),nb_serv=sum(nb_serv),nb_metr=sum(nb_metr),nb_contr=sum(nb_contr)) %>%
     ungroup() %>% as.data.frame() %>% rename(zone=allvar)

VueGlobale2 <- rbind(VueGlobale,Vue_Auzeville,Vue_Osny,Vue_Total)
rm(Vue_Auzeville,Vue_Osny,Vue_Total)

VueGlobale3 <- VueGlobale2 %>% 
     mutate(nb_contr_par_hote=round(nb_contr/nb_hote,0),
            nb_contr_par_serv=round(nb_contr/nb_serv,0),
            nb_contr_par_metr=round(nb_contr/nb_metr,0),
            
            part_serv_1 = round((nb_serv/VueGlobale2$nb_serv[11]*100),0),
            part_serv_2 = round((nb_serv/(VueGlobale2$nb_serv[2]+
                                          VueGlobale2$nb_serv[4]+
                                          VueGlobale2$nb_serv[6]+
                                          VueGlobale2$nb_serv[7]+
                                          VueGlobale2$nb_serv[8])*100),0))
VueGlobale3$part_serv_2[1] <- NA
VueGlobale3$part_serv_2[3] <- NA
VueGlobale3$part_serv_2[5] <- NA
VueGlobale3$part_serv_2[9] <- NA
VueGlobale3$part_serv_2[10] <- NA
VueGlobale3$part_serv_2[11] <- 100

write.csv2(VueGlobale3, 
           paste0(folder,"/VueGlobale_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)

####

Req_HGHSM_export <- Req_HGHSM %>% 
     select(zone, Gr_Hotes, name_host, description, metric_name, 
            check_interval, nbcontr_jr, unit_name, warn, crit) %>% 
     mutate(warn=ifelse(is.na(warn),"",warn),
            crit=ifelse(is.na(crit),"",crit))
# Export en .csv
write.csv2(Req_HGHSM_export,
           paste0(folder,"/HGHSM_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)

gc()

Vue_NivGrHote <- Req_HGHSM_export %>% group_by(zone,Gr_Hotes) %>% 
     summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1)

Vue_NivHote <- Req_HGHSM_export %>% group_by(zone,Gr_Hotes,name_host) %>% 
     summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1)

Vue_NivService <- Req_HGHSM_export %>% group_by(zone,Gr_Hotes,name_host,description) %>% 
     summarise(nb_obs=n()) %>%  ungroup() %>% as.data.frame() %>% mutate(nb_obs=1)


# Exports en .csv
write.csv2(Vue_NivGrHote,
           paste0(folder,"/Agreg_NivGrHote_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)
write.csv2(Vue_NivHote,
           paste0(folder,"/Agreg_NivHote_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)
write.csv2(Vue_NivService,
           paste0(folder,"/Agreg_NivService_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)

# nettoyage RAM...
gc()

setwd(folder)

# liste des fichiers csv presents dans le repertoire du jour et 
# enregistrement dans un dataframe listing puis concatenation dans un fichier .zip
listing <- list.files(folder) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich)

# insertion du fichier unitaire dans un .zip puis suppression
zip(paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip"),
    paste0(listing$liste_fich[1]))
zip(paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip"),
    paste0(listing$liste_fich[2]))
zip(paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip"),
    paste0(listing$liste_fich[1]))
zip(paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip"),
    paste0(listing$liste_fich[3]))
zip(paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip"),
    paste0(listing$liste_fich[4]))
zip(paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip"),
    paste0(listing$liste_fich[5]))

# copie vers le repertoire de transfert sas ver Zone 50...

rep_sas <- "C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/"
fichier_zip <- paste0("Recensement_",str_sub(lib_date_jr,3,16),".zip")

file.copy(fichier_zip,
          paste0(rep_sas,fichier_zip))
