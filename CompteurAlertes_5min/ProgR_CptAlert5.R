
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

rm(part_date,part_heure,part_minute,part_seconde)

##################################################
##                                              ##
## 1/ PARTIE REQUETE DE LA BDD CENTREON         ##
##                                              ##
################################################## ----
##
## Connexion a la nouvelle base Centreon EDIFICE Osny 100 ----
## maitre 172.26.30.11 ; esclave 172.26.30.12
##
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon", host="172.26.30.11", port="3306")

# !! utilisation de la base centreon (tout court) et non centreon.storage

##################################################################

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
## maitre 172.26.30.11 ; esclave 172.26.30.12
##
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon_storage", host="172.26.30.11", port="3306")

# !! utilisation de la base centreon_storage ici pour recuperer le niveau metrique

##################################################################

Req_SM <- dbGetQuery(baseMdb,
                     paste("SELECT s.service_id, s.description, s.enabled as active_service,
                                  s.check_command, s.command_line, s.check_period, s.check_interval,
                                  s.acknowledged,
                                  s.last_notification, 
                                  s.last_check, 
                                  s.last_hard_state, s.last_hard_state_change,
                                  s.last_state_change, 
                                  s.last_time_ok, s.last_time_warning, 
                                  s.last_time_critical, s.last_time_unknown,  
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
          ".insee.fr$|.dmz.insee$|.part.insee$|.insee.intra$|.ad.insee.intra$|.adsia.sia.priv$|.dc1.sia.priv$|.dc2.sia.priv$|.dr.sia.priv$",
          "")) %>% 
     relocate(name_host_2,.after="name_host") %>% 
     select(-name_host) %>% rename(name_host=name_host_2)

# fusion entre les df Hote-Service et Service-Metrique
Req_HSM <- Req_HS_2 %>% 
     left_join(Req_SM, by=c("service_id")) %>% select(-i_id,-service_id_index)



Req_GH_2 <- Req_GH %>% 
     mutate(name_host_2=str_replace(name_host,
          ".insee.fr$|.dmz.insee$|.part.insee$|.insee.intra$|.ad.insee.intra$|.adsia.sia.priv$|.dc1.sia.priv$|.dc2.sia.priv$|dr.sia.priv$",
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
# on trouve 1 seul hote coorespondant a census.insee.net (Services_Ressources_Externes_INSEE)


## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

# fusion entre les df Hote-Service-Metrique et Groupe Hote-Hote (Vue3 !)
Req_HGHSM <- Req_HSM %>% 
     left_join(Vue3, by=c("name_host")) %>% relocate(Gr_Hotes,.after="host_address")


Req_HGHSM <- Req_HGHSM %>%
     mutate(zone = case_when(
          name_poller %in% c("PollerAuzeville00","Poller1Auzeville00")  ~ "01_DMZ_Auze",
          name_poller %in% c("PollerOsny00","Poller1Osny00")            ~ "02_DMZ_Osny",
          name_poller %in% c("PollerAuzeville25")                       ~ "03_Part_Auze",
          name_poller %in% c("PollerOsny25")                            ~ "04_Part_Osny",
          name_poller %in% c("PollerAuzeville50","Poller1Auzeville50",
                             "Poller2Auzeville50","Poller3Auzeville50") ~ "05_ZoneInt_Auze",
          name_poller %in% c("PollerOsny50","Poller1Osny50",
                             "Poller2Osny50","RsOsny50")                ~ "06_ZoneInt_Osny",
          name_poller %in% c("RsAuzeville100","PollerAuzeville100")     ~ "07_ZoneSIA_Auze",
          name_poller %in% c("Central","PollerOsny100")                 ~ "08_ZoneSIA_Osny",
          TRUE ~ "09_Autre")) %>% 
     relocate(zone,.after=name_poller) %>% 
     mutate(nbcontr_jr = 1440 / check_interval) %>% relocate(nbcontr_jr,.after=check_interval)



# il y a quelques controles pour lesquels le check_interval n'est pas renseigne, 
# on les retire car peu nombreux
visu <- Req_HGHSM %>% filter(is.na(nbcontr_jr))

Nbre_Service <- Req_HGHSM %>% group_by(poller_id, zone, host_id, name_host, service_id, description) %>% 
     summarise(nb_metr=n()) %>%  ungroup() %>% as.data.frame()

Req_HGHSM_bis <- Req_HGHSM %>% select(-m_id,-metric_name,-unit_name,-warn,-crit) %>% 
     unique()

###

DataReq <- Req_HGHSM_bis %>% 
     select(poller_id:Gr_Hotes,
            service_id,description,check_interval,acknowledged,
            last_check,last_hard_state,last_hard_state_change) %>% 
     filter(last_hard_state %in% c(1,2,3)) %>% 
     mutate(moment=str_sub(lib_date_jr,3,16)) %>%
     relocate(moment)

####################################################
####################################################
# definition fonction dagregation par Datacenter
fct_Agreg <- function(df,DataCenterRef){
     df_out <- df %>% group_by(zone,acknowledged,last_hard_state) %>% 
          summarise(nb_status=n()) %>% ungroup() %>% as.data.frame() %>% 
          filter(str_detect(zone,DataCenterRef))
     
     df_out <- df_out %>% 
          mutate(acq=case_when(
               acknowledged==0  ~ "Acq_Non",
               acknowledged==1  ~ "Acq_Oui",
               TRUE ~ "")) %>% 
          mutate(status = case_when(
               last_hard_state==0  ~ "Res4_Ok",
               last_hard_state==1  ~ "Res2_Warn",
               last_hard_state==2  ~ "Res1_Crit",
               last_hard_state==3  ~ "Res3_Inc",
               TRUE ~ "")) %>% filter(last_hard_state!=0) %>% 
          select(-acknowledged,-last_hard_state) %>% arrange(status)
     
     df_out <- pivot_wider(df_out, names_from=status, values_from = nb_status) %>%
          as.data.frame() %>% arrange(acq) %>% 
          rename(Critical=Res1_Crit,Warning=Res2_Warn,Unknown=Res3_Inc)
     
     if (DataCenterRef=="Osny"){
          zone <- rep(c("02_DMZ_Osny","04_Part_Osny","06_ZoneInt_Osny","08_ZoneSIA_Osny"),times=2)
     } else {
          zone <- rep(c("01_DMZ_Auze","03_Part_Auze","05_ZoneInt_Auze","07_ZoneSIA_Auze"),times=2)
     }
     acq <- c(rep("Acq_Non",times=4),rep("Acq_Oui",times=4))
     df_comb <- data.frame(zone,acq)
     df_out <- df_out %>% right_join(df_comb, by=c("zone","acq")) %>% 
          arrange(acq,zone) %>% 
          mutate(Critical=ifelse(is.na(Critical),0,Critical),
                 Warning=ifelse(is.na(Warning),0,Warning),
                 Unknown=ifelse(is.na(Unknown),0,Unknown))
     
     rm(zone,acq)

     Rep_statut_tot <- df_out %>%  
          summarise(Critical=sum(Critical),Warning=sum(Warning),Unknown=sum(Unknown))
     if (DataCenterRef=="Osny"){
          Rep_statut_tot <- Rep_statut_tot %>% 
               mutate(acq="Ensemble_Osny") %>% relocate(acq) %>% 
               mutate(zone="Ensemble_Osny") %>% relocate(zone)
     } else {
          Rep_statut_tot <- Rep_statut_tot %>% 
               mutate(acq="Ensemble_Auze") %>% relocate(acq) %>% 
               mutate(zone="Ensemble_Auze") %>% relocate(zone)
     }
     df_out <- rbind(df_out,Rep_statut_tot)
     rm(Rep_statut_tot)
     
     df_out <- df_out %>% mutate(moment=str_sub(lib_date_jr,3,16)) %>%
          relocate(moment) %>% 
          arrange(acq,zone)
     
     return(df_out)
}

Rep_statut_DC1 <- fct_Agreg(Req_HGHSM_bis,"Osny")
Rep_statut_DC2 <- fct_Agreg(Req_HGHSM_bis,"Auze")


########################################################
########################################################
# sauvegarde des resultats dans un repertoire Windows

# Creation dun repertoire avec la date du jour
folder <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",format(Sys.time(),'%Y%m%d'))
if (file.exists(folder)) {
     cat("The folder already exists")
} else {
     dir.create(folder, showWarnings = TRUE, recursive = FALSE, mode = "0777")
}

write.csv2(Rep_statut_DC1, 
           paste0(folder,"/df_status_DC1_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)
write.csv2(Rep_statut_DC2,
           paste0(folder,"/df_status_DC2_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)
write.csv2(DataReq,
           paste0(folder,"/df_datareq_",str_sub(lib_date_jr,3,16),".csv"),
           row.names=FALSE)





