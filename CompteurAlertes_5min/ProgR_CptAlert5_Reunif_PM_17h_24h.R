
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

date_jour <- Sys.time()
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
lib_time <- paste0("_",part_heure,"h",part_minute)

rm(part_heure,part_minute)

part_date  <- as.character(Sys.Date() - ddays(1))
part_date2 <- str_remove_all(part_date,"-")

# pour tester...
# part_date2 <- "20230707"

rep_fich <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",part_date2,"/")
setwd(rep_fich)

rep_sas <- paste0("C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/")


##########
##########
# liste des fichiers csv presents dans le repertoire sas et 
# enregistrement dans un dataframe listing
listing <- list.files(rep_fich) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich)

listing_datareq <- listing %>%
     filter(str_detect(liste_fich,"datareq") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)
listing_dc1dc2 <- listing %>%
     filter(str_detect(liste_fich,"status_DC1") | str_detect(liste_fich,"status_DC2")) %>%
     arrange(liste_fich)

# filtre complementaire sur la periode de temps considere
vect_periode <- c("17h00","17h05","17h10","17h15","17h20","17h25",
                  "17h30","17h35","17h40","17h45","17h50","17h55",
                  "18h00","18h05","18h10","18h15","18h20","18h25",
                  "18h30","18h35","18h40","18h45","18h50","18h55",
                  "19h00","19h05","19h10","19h15","19h20","19h25",
                  "19h30","19h35","19h40","19h45","19h50","19h55",
                  "20h00","20h05","20h10","20h15","20h20","20h25",
                  "20h30","20h35","20h40","20h45","20h50","20h55",
                  "21h00","21h05","21h10","21h15","21h20","21h25",
                  "21h30","21h35","21h40","21h45","21h50","21h55",
                  "22h00","22h05","22h10","22h15","22h20","22h25",
                  "22h30","22h35","22h40","22h45","22h50","22h55",
                  "23h00","23h05","23h10","23h15","23h20","23h25",
                  "23h30","23h35","23h40","23h45","23h50","23h55")
                  
listing_datareq <- listing_datareq %>% filter(str_sub(liste_fich,21,25) %in% vect_periode)
listing_dc1dc2 <- listing_dc1dc2 %>% filter(str_sub(liste_fich,24,28) %in% vect_periode)                                            
                                             
                                             
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
     #file.remove(paste0(rep_fich,listing_datareq$liste_fich[i]))
}
rm(i,df_copy)

# ajout des 2 variables poids et unite
df_datareq <- read.csv2(paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"))
df_datareq <- df_datareq %>% 
     mutate(unite=1) %>% relocate(unite) %>% 
     mutate(poids = case_when(
          last_hard_state==3  ~ 1,
          last_hard_state==1  ~ 2,
          last_hard_state==2  ~ 4,
          TRUE ~ 0)) %>% relocate(poids)

# ajout des variables moment_30m moment_1h et ampm 
df_datareq <- df_datareq %>% 
     mutate(moment_30m=str_sub(moment,10,15)) %>% relocate(moment_30m,.before=moment) %>% 
     mutate(moment_30m = case_when(
          str_sub(moment_30m,4,5) %in% c("00","05","10","15","20","25") ~ paste0(str_sub(moment_30m,1,3),"00"),
          str_sub(moment_30m,4,5) %in% c("30","35","40","45","50","55") ~ paste0(str_sub(moment_30m,1,3),"30"),
          TRUE ~ "XX")) %>% 
     mutate(moment_1h=str_sub(moment_30m,1,3)) %>% relocate(moment_1h,.before=moment_30m) %>% 
     mutate(ampm=case_when(
          moment_1h %in% c("00h","01h","02h","03h","04h","05h",
                           "06h","07h","08h","09h","10h","11h") ~ "AM",
          moment_1h %in% c("12h","13h","14h","15h","16h","17h",
                           "18h","19h","20h","21h","22h","23h") ~ "PM",
          TRUE ~ "XX")) %>% relocate(ampm,.before=moment_1h)

write.csv2(df_datareq,paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv"),row.names=FALSE)


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
     #file.remove(paste0(rep_fich,listing_dc1dc2$liste_fich[i]))
}
rm(i,df_copy)

# lib_time <- "_01h30"

# Transfert des fichiers dans le repertoire sas vers la z050
df_datajreq <- read.csv2(paste0(rep_fich,"df_datajreq_",part_date,lib_time,".csv")) %>% 
     select(-host_address)
write.csv2(df_datajreq, 
           paste0(rep_sas,"df_datajreq_",part_date,lib_time,".csv"),
           row.names=FALSE)

df_statusj <- read.csv2(paste0(rep_fich,"df_statusj_",part_date,lib_time,".csv"))
write.csv2(df_statusj, 
           paste0(rep_sas,"df_statusj_",part_date,lib_time,".csv"),
           row.names=FALSE)


