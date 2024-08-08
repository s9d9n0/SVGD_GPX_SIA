
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

# decalage egal de 2 en heure en ete et 1 en heure en hiver
decalage_heure <- 2

date_jour <- Sys.time()
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
lib_time <- paste0("_",part_heure,"h",part_minute)

# pour tester
# lib_time <- "_01h30"

rm(part_heure,part_minute)

part_date  <- as.character(Sys.Date()-ddays(1))
part_date2 <- str_remove_all(part_date,"-")

# pour tester...
# part_date2 <- "20230823"

rep_fich <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",part_date2,"/")
setwd(rep_fich)

rep_sas <- paste0("C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/")


##########
##########
# liste des fichiers csv presents dans le repertoire sas et 
# enregistrement dans un dataframe listing
listing <- list.files(rep_fich) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich)

# modification du 04 nov : 
# ...sur listing_datareq, datajreq au lieu de datareq
listing_datareq <- listing %>%
     filter(str_detect(liste_fich,"datajreq") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)

listing_dc1dc2 <- listing %>%
     filter(str_detect(liste_fich,"status_DC1") | str_detect(liste_fich,"status_DC2")) %>%
     arrange(liste_fich)


##########
##########
# PARTIE 1
# copie de tous les fichiers datareq dans le repertoire de reference

for (i in 1:nrow(listing_datareq)){
     cat(paste0("numero ",i," ",listing_datareq$liste_fich[i],"\n"))
# concatenation des fichiers de type datareq
     if (i==1) {
          file.copy(paste0(rep_fich,listing_datareq$liste_fich[i]),
                    paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"))
     } else {
          df_datareq <- read.csv2(paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"))
          df_copy <- read.csv2(paste0(rep_fich,listing_datareq$liste_fich[i]))
          df_datareq <- rbind(df_datareq,df_copy)
          write.csv2(df_datareq,paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"),row.names=FALSE)
     }
# insertion du fichier unitaire dans un .zip puis suppression
     zip(paste0("df_datajreq_",part_date,lib_time,".zip"),
         paste0(listing_datareq$liste_fich[i]))
     # file.remove(paste0(rep_fich,listing_datareq$liste_fich[i]))
}
rm(i,df_copy)

#tests pour ajout
# rep_fich <- "C:/Users/siar_ycg8l6/ProgrammesR_Gepex/CompteurAlertes_5min/Fichiers_date_20231018/"
# part_date <- "2023-10-18"
# lib_time <- "_01h30"

# # AJOUT des 2 variables poids et unite
# df_datareq <- read.csv2(paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"))
# df_datareq <- df_datareq %>% 
#      mutate(unite=1) %>% relocate(unite) %>% 
#      mutate(poids = case_when(
#           last_hard_state==3  ~ 1,
#           last_hard_state==1  ~ 2,
#           last_hard_state==2  ~ 4,
#           TRUE ~ 0)) %>% relocate(poids)

# AJOUT des 2 variables date_jour et date_heure
# df_datareq <- df_datareq %>% 
#      mutate(date_jour=str_sub(moment,1,8)) %>% relocate(date_jour,.before="moment") %>% 
#      mutate(date_heure=str_sub(moment,10,11)) %>% relocate(date_heure,.before="moment")



# timZon <- OlsonNames()
# vue_tz <- with_tz(as_datetime(Sys.time()),tz=Sys.timezone())
# vue_utc <- with_tz(as_datetime(Sys.time()),"UTC")
# vuesec_tz <- as.integer(vue_tz) - as.integer(vue_utc)
# vue_now <- strptime(now(),"%Y-%m-%d %H-%M-%S")
# vue_now <- datetime_to_timestamp(now())

# df_datareq <- df_datareq %>%
#      mutate(duree_tot=floor((last_check-last_hard_state_change)/60)) %>% 
#      mutate(duree_minut_reste=as.character(duree_tot%%60)) %>% 
#      mutate(duree_minut_reste=ifelse(str_length(duree_minut_reste)==1,
#                                      paste0("0",duree_minut_reste),
#                                      duree_minut_reste)) %>% 
#      mutate(duree_heure=duree_tot%/%60) %>% 
#      mutate(duree_heure_reste=as.character(duree_heure%%24)) %>% 
#      mutate(duree_heure_reste=ifelse(str_length(duree_heure_reste)==1,
#                                      paste0("0",duree_heure_reste),
#                                      duree_heure_reste)) %>%     
#      mutate(duree_jour=duree_heure%/%24) %>%      
#      mutate(duree_jour_reste=as.character(duree_jour%%7)) %>% 
#      mutate(duree_jour_reste=ifelse(str_length(duree_jour_reste)==1,
#                                     paste0("0",duree_jour_reste),
#                                     duree_jour_reste)) %>%  
#      mutate(duree_sem=duree_jour%/%7) %>%
#      mutate(duree_sem_reste=as.character(duree_sem%%52)) %>% 
#      mutate(duree_sem_reste=ifelse(str_length(duree_sem_reste)==1,
#                                    paste0("0",duree_sem_reste),
#                                    duree_sem_reste)) %>%
#      mutate(duree_an=duree_sem%/%52) %>% 
#      mutate(duree_an=ifelse(str_length(duree_an)==1,
#                             paste0("0",duree_an),
#                             duree_an)) %>%
#      mutate(duree_vision=paste0(duree_an,"an ",
#                                 duree_sem_reste,"se ",
#                                 duree_jour_reste,"j ",
#                                 duree_heure_reste,"h ",
#                                 duree_minut_reste,"m")) %>% 
#      select(-duree_heure,-duree_jour,-duree_sem) %>% 
#      rename(dur_min=duree_minut_reste,
#             dur_hr=duree_heure_reste,
#             dur_jr=duree_jour_reste,
#             dur_sem=duree_sem_reste)
#      
# df_datareq <- df_datareq %>%
#      mutate(moment_dern_ctrl=as_datetime(last_check)+dhours(decalage_heure)) %>% 
#      mutate(moment_dern_chg_etat=as_datetime(last_hard_state_change)+dhours(decalage_heure))


#write.csv2(df_datareq,paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"),row.names=FALSE)

# nettoyage RAM...
gc()


##########
##########
# PARTIE 2
# copie de tous les fichiers status_DC1 ou status_DC2 dans le repertoire de reference
for (i in 1:nrow(listing_dc1dc2)){
     cat(paste0("numero ",i," ",listing_dc1dc2$liste_fich[i],"\n"))
     # concatenation des fichiers de type status_DC
     if (i==1) {
          file.copy(paste0(rep_fich,listing_dc1dc2$liste_fich[i]),
                    paste0(rep_fich,"df_statusj_",part_date,lib_time,".csv"))
     } else {
          df_status <- read.csv2(paste0(rep_fich,"df_statusj_",part_date,lib_time,".csv"))
          df_copy <- read.csv2(paste0(rep_fich,listing_dc1dc2$liste_fich[i]))
          df_status <- rbind(df_status,df_copy)
          write.csv2(df_status,paste0(rep_fich,"df_statusj_",part_date,lib_time,".csv"),row.names=FALSE)
     }
     # insertion du fichier unitaire dans un .zip puis suppression
     zip(paste0("df_statusj_",part_date,lib_time,".zip"),
         paste0(listing_dc1dc2$liste_fich[i]))
     file.remove(paste0(rep_fich,listing_dc1dc2$liste_fich[i]))
}
rm(i,df_copy)

# nettoyage RAM...
gc()


# lib_time <- "_01h30"

# TRANSFERT des fichiers dans le repertoire sas vers la z050
df_datajreq <- read.csv2(paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv")) %>%
     select(-host_address)
write.csv2(df_datajreq,
           paste0(rep_sas,"df_datajreq_",part_date,lib_time,".csv"),
           row.names=FALSE)

df_statusj <- read.csv2(paste0(rep_fich,"df_statusj_",part_date,lib_time,".csv"))
write.csv2(df_statusj,
           paste0(rep_sas,"df_statusj_",part_date,lib_time,".csv"),
           row.names=FALSE)


