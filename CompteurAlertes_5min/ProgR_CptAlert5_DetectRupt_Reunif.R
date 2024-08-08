
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

part_date  <- as.character(Sys.Date()-ddays(1))
part_date2 <- str_remove_all(part_date,"-")

part_date_aujourdhui  <- as.character(Sys.Date())
part_date2_aujourdhui <- str_remove_all(part_date_aujourdhui,"-")

# pour tester...
# part_date2 <- "20240504"
# part_date2_aujourdhui <- "20240505"
# lib_time <- "_06h35"

rep_fich <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",part_date2,"_delta_5min/")

rep_fich_aujourdhui <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",part_date2_aujourdhui,"_delta_5min/")

rep_sas <- paste0("C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/")


##########
##########
# liste des fichiers csv presents dans le repertoire fich d'hier et 
# enregistrement dans un dataframe listing

setwd(rep_fich)

listing <- list.files(rep_fich) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich)

listing_delta_group <- listing %>%
     filter(str_detect(liste_fich,"df_delta_group_2") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)
listing_regroup <- listing %>%
     filter(str_detect(liste_fich,"df_regroup_2") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)


##########
##########
# PARTIE 1-A
# copie de tous les fichiers datareq dans le repertoire de reference
for (i in 1:nrow(listing_delta_group)){
     cat(paste0("numero ",i," ",listing_delta_group$liste_fich[i],"\n"))
     # concatenation des fichiers de type delta_group
     if (i==1) {
          file.copy(paste0(rep_fich,listing_delta_group$liste_fich[i]),
                    paste0(rep_fich,"df_delta_group_jour_",part_date,lib_time,".csv"))
     } else {
          df_datareq <- read.csv2(paste0(rep_fich,"df_delta_group_jour_",part_date,lib_time,".csv"))
          df_copy <- read.csv2(paste0(rep_fich,listing_delta_group$liste_fich[i]))
          df_datareq <- rbind(df_datareq,df_copy)
          write.csv2(df_datareq,paste0(rep_fich,"df_delta_group_jour_",part_date,lib_time,".csv"),row.names=FALSE)
     }
     # insertion du fichier unitaire dans un .zip puis suppression
     zip(paste0("df_delta_group_jour_",part_date,lib_time,".zip"),
         paste0(listing_delta_group$liste_fich[i]))
     file.remove(paste0(rep_fich,listing_delta_group$liste_fich[i]))
}
rm(i,df_copy)

write.csv2(df_datareq,
           paste0(rep_fich,"df_delta_group_jour_",part_date,lib_time,".csv"),
           row.names=FALSE)

# Transfert egalement des fichiers dans le repertoire sas vers la z050
write.csv2(df_datareq,
           paste0(rep_sas,"df_delta_group_jour_",part_date,lib_time,".csv"),
           row.names=FALSE)


##########
##########
# PARTIE 2-A
# copie de tous les fichiers datareq dans le repertoire de reference
# for (i in 1:nrow(listing_regroup)){
#      cat(paste0("numero ",i," ",listing_regroup$liste_fich[i],"\n"))
#      # concatenation des fichiers de type regroup
#      if (i==1) {
#           file.copy(paste0(rep_fich,listing_regroup$liste_fich[i]),
#                     paste0(rep_fich,"df_regroup_jour_",part_date,lib_time,".csv"))
#      } else {
#           df_datareq <- read.csv2(paste0(rep_fich,"df_regroup_jour_",part_date,lib_time,".csv"))
#           df_copy <- read.csv2(paste0(rep_fich,listing_regroup$liste_fich[i]))
#           df_datareq <- rbind(df_datareq,df_copy)
#           write.csv2(df_datareq,paste0(rep_fich,"df_regroup_jour_",part_date,lib_time,".csv"),row.names=FALSE)
#      }
     # insertion du fichier unitaire dans un .zip puis suppression
#      zip(paste0("df_regroup_jour_",part_date,lib_time,".zip"),
#          paste0(listing_regroup$liste_fich[i]))
#      file.remove(paste0(rep_fich,listing_regroup$liste_fich[i]))
# }
# rm(i,df_copy)

# ajout de la variables unite
# df_datareq <- read.csv2(paste0(rep_fich,"df_regroup_jour_",part_date,lib_time,".csv"))
# df_datareq <- df_datareq %>% 
#      mutate(unite=1) %>% relocate(unite)

# write.csv2(df_datareq,
#            paste0(rep_fich,"df_regroup_jour_",part_date,lib_time,".csv"),
#            row.names=FALSE)

# Transfert egalement des fichiers dans le repertoire sas vers la z050
# write.csv2(df_datareq,
#            paste0(rep_sas,"df_regroup_jour_",part_date,lib_time,".csv"),
#            row.names=FALSE)

###########################################################################
###########################################################################
###########################################################################
###########################################################################


# liste des fichiers csv presents dans le repertoire fich de la matinee meme et 
# enregistrement dans un dataframe listing_aujourdhui

setwd(rep_fich_aujourdhui)

listing_aujourdhui <- list.files(rep_fich_aujourdhui) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich)

listing_delta_group_aujourdhui <- listing_aujourdhui %>%
     filter(str_detect(liste_fich,"df_delta_group_2") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)
listing_regroup_aujourdhui <- listing_aujourdhui %>%
     filter(str_detect(liste_fich,"df_regroup_2") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)


##########
##########
# PARTIE 1-B
# copie de tous les fichiers datareq dans le repertoire de reference
for (i in 1:nrow(listing_delta_group_aujourdhui)){
     cat(paste0("numero ",i," ",listing_delta_group_aujourdhui$liste_fich[i],"\n"))
     # concatenation des fichiers de type delta_group
     if (i==1) {
          file.copy(paste0(rep_fich_aujourdhui,listing_delta_group_aujourdhui$liste_fich[i]),
                    paste0(rep_fich_aujourdhui,"df_delta_group_jour_",part_date_aujourdhui,lib_time,".csv"))
     } else {
          df_datareq <- read.csv2(paste0(rep_fich_aujourdhui,"df_delta_group_jour_",part_date_aujourdhui,lib_time,".csv"))
          df_copy <- read.csv2(paste0(rep_fich_aujourdhui,listing_delta_group_aujourdhui$liste_fich[i]))
          df_datareq <- rbind(df_datareq,df_copy)
          write.csv2(df_datareq,paste0(rep_fich_aujourdhui,"df_delta_group_jour_",part_date_aujourdhui,lib_time,".csv"),row.names=FALSE)
     }
     # insertion du fichier unitaire dans un .zip puis suppression
     #zip(paste0("df_delta_group_jour_",part_date_aujourdhui,lib_time,".zip"),
     #    paste0(listing_delta_group_aujourdhui$liste_fich[i]))
     #file.remove(paste0(rep_fich_aujourdhui,listing_delta_group_aujourdhui$liste_fich[i]))
}
rm(i,df_copy)

write.csv2(df_datareq,
           paste0(rep_fich_aujourdhui,"df_delta_group_jour_",part_date_aujourdhui,lib_time,".csv"),
           row.names=FALSE)

# Transfert egalement des fichiers dans le repertoire sas vers la z050
write.csv2(df_datareq,
           paste0(rep_sas,"df_delta_group_jour_",part_date_aujourdhui,lib_time,".csv"),
           row.names=FALSE)


##########
##########
# PARTIE 2-B
# copie de tous les fichiers datareq dans le repertoire de reference
# for (i in 1:nrow(listing_regroup_aujourdhui)){
#      cat(paste0("numero ",i," ",listing_regroup_aujourdhui$liste_fich[i],"\n"))
#      # concatenation des fichiers de type regroup
#      if (i==1) {
#           file.copy(paste0(rep_fich_aujourdhui,listing_regroup_aujourdhui$liste_fich[i]),
#                     paste0(rep_fich_aujourdhui,"df_regroup_jour_",part_date_aujourdhui,lib_time,".csv"))
#      } else {
#           df_datareq <- read.csv2(paste0(rep_fich_aujourdhui,"df_regroup_jour_",part_date_aujourdhui,lib_time,".csv"))
#           df_copy <- read.csv2(paste0(rep_fich_aujourdhui,listing_regroup_aujourdhui$liste_fich[i]))
#           df_datareq <- rbind(df_datareq,df_copy)
#           write.csv2(df_datareq,paste0(rep_fich_aujourdhui,"df_regroup_jour_",part_date_aujourdhui,lib_time,".csv"),row.names=FALSE)
#      }
     # insertion du fichier unitaire dans un .zip puis suppression
     #zip(paste0("df_regroup_jour_",part_date_aujourdhui,lib_time,".zip"),
     #    paste0(listing_regroup_aujourdhui$liste_fich[i]))
     #file.remove(paste0(rep_fich_aujourdhui,listing_regroup_aujourdhui$liste_fich[i]))
# }
# rm(i,df_copy)

# ajout de la variables unite
# df_datareq <- read.csv2(paste0(rep_fich_aujourdhui,"df_regroup_jour_",part_date_aujourdhui,lib_time,".csv"))
# df_datareq <- df_datareq %>% 
#      mutate(unite=1) %>% relocate(unite)
# 
# write.csv2(df_datareq,
#            paste0(rep_fich_aujourdhui,"df_regroup_jour_",part_date_aujourdhui,lib_time,".csv"),
#            row.names=FALSE)

# Transfert egalement des fichiers dans le repertoire sas vers la z050
# write.csv2(df_datareq,
#            paste0(rep_sas,"df_regroup_jour_",part_date_aujourdhui,lib_time,".csv"),
#            row.names=FALSE)


##############################################################################
##############################################################################
##############################################################################

##########
##########
# PARTIE 3
# analyse des tables reunies
# df_regroup_jour <- read.csv2(paste0(rep_fich,"df_regroup_jour_",part_date,"_06h35",".csv"))
# df_etud_regroup_jour <- df_regroup_jour %>% arrange(zone,Gr_Hotes)

df_delta_group_jour <- read.csv2(paste0(rep_fich,"df_delta_group_jour_",part_date,"_06h35",".csv"))
df_etud_delta_group_jour <- df_delta_group_jour %>% 
     arrange(zone,Gr_Hotes,name_host,description) %>% filter(chgt=="chgt")

df_delta_group_jour_aujourdhui <- read.csv2(paste0(rep_fich_aujourdhui,"df_delta_group_jour_",part_date_aujourdhui,"_06h35",".csv"))
df_etud_delta_group_jour_aujourdhui <- df_delta_group_jour_aujourdhui %>% 
     arrange(zone,Gr_Hotes,name_host,description) %>% filter(chgt=="chgt")

df_delta_group_jour_reunis <- rbind(df_delta_group_jour,
                                    df_delta_group_jour_aujourdhui)

rm(df_delta_group_jour,
   df_delta_group_jour_aujourdhui)

##################################
# ANALYSE 
Vue1 <- df_delta_group_jour_reunis %>%
     filter(diff_sur_statut %in% c("1_Crit","2_Warn"))
# Vue <- Vue1 %>% filter(status_5!="1_Crit" & status_0!="1_Crit")

Vue1 <- Vue1 %>% 
     filter( (status_5 %in% c("1_Crit","2_Warn") | status_0 %in% c("1_Crit","2_Warn")) ) %>% 
     # filter((status_5=="1_Crit" | status_0=="1_Crit")) %>% 
     # filter((status_5=="1_Crit" | status_0=="1_Crit") & 
     #        (!str_detect(Gr_Hotes,"-Etab") & !str_detect(Gr_Hotes,"-Selenium"))) %>% 
     select(moment,zone,Gr_Hotes,name_host,description,status_0) %>% 
     arrange(moment,zone,Gr_Hotes,name_host,description) %>% 
     mutate(moment=paste0("_",str_sub(moment,6,16)))

# Vue1 <- df_etud_delta_group_jour %>% 
#      select(moment,diff_sur_statut,zone,Gr_Hotes,name_host,description,status_0) %>% 
#      arrange(moment,zone,Gr_Hotes,name_host,description) %>% 
#      mutate(moment=paste0("_",str_sub(moment,12,16)))


# Vue <- Vue1 %>% filter(name_host=="pprcageolas73")

# recuperation du nbre de changement de statut

# ancienne methode visiblement fausse...
# Vue_nbchgt <- Vue1 %>% group_by(zone,Gr_Hotes,name_host,description) %>%
#      summarise(nbchgt=n()) %>% ungroup() %>% as.data.frame()
# Vue1 <- Vue1 %>% left_join(Vue_nbchgt, by=c("zone","Gr_Hotes","name_host","description")) %>%
#      arrange(moment,zone,Gr_Hotes,name_host,description)

Vue_nbchgt <- df_delta_group_jour_reunis %>% filter(chgt=="chgt") %>%
     select(moment,zone,Gr_Hotes,name_host,description)
Vue_nbchgt <- Vue_nbchgt %>% filter(duplicated(Vue_nbchgt)) %>% 
     group_by(zone,Gr_Hotes,name_host,description) %>% 
     summarise(nbchgt=n()) %>% ungroup() %>% as.data.frame()
Vue1 <- Vue1 %>% left_join(Vue_nbchgt, by=c("zone","Gr_Hotes","name_host","description")) %>% 
     arrange(moment,zone,Gr_Hotes,name_host,description)


vectheure <- c("_00h00","_00h05","_00h10","_00h15","_00h20","_00h25","_00h30","_00h35","_00h40","_00h45","_00h50","_00h55",
               "_01h00","_01h05","_01h10","_01h15","_01h20","_01h25","_01h30","_01h35","_01h40","_01h45","_01h50","_01h55",
               "_02h00","_02h05","_02h10","_02h15","_02h20","_02h25","_02h30","_02h35","_02h40","_02h45","_02h50","_02h55",
               "_03h00","_03h05","_03h10","_03h15","_03h20","_03h25","_03h30","_03h35","_03h40","_03h45","_03h50","_03h55",
               "_04h00","_04h05","_04h10","_04h15","_04h20","_04h25","_04h30","_04h35","_04h40","_04h45","_04h50","_04h55",
               "_05h00","_05h05","_05h10","_05h15","_05h20","_05h25","_05h30","_05h35","_05h40","_05h45","_05h50","_05h55",
               "_06h00","_06h05","_06h10","_06h15","_06h20","_06h25","_06h30","_06h35","_06h40","_06h45","_06h50","_06h55",
               "_07h00","_07h05","_07h10","_07h15","_07h20","_07h25","_07h30","_07h35","_07h40","_07h45","_07h50","_07h55",
               "_08h00","_08h05","_08h10","_08h15","_08h20","_08h25","_08h30","_08h35","_08h40","_08h45","_08h50","_08h55",
               "_09h00","_09h05","_09h10","_09h15","_09h20","_09h25","_09h30","_09h35","_09h40","_09h45","_09h50","_09h55",
               "_10h00","_10h05","_10h10","_10h15","_10h20","_10h25","_10h30","_10h35","_10h40","_10h45","_10h50","_10h55",
               "_11h00","_11h05","_11h10","_11h15","_11h20","_11h25","_11h30","_11h35","_11h40","_11h45","_11h50","_11h55",
               "_12h00","_12h05","_12h10","_12h15","_12h20","_12h25","_12h30","_12h35","_12h40","_12h45","_12h50","_12h55",
               "_13h00","_13h05","_13h10","_13h15","_13h20","_13h25","_13h30","_13h35","_13h40","_13h45","_13h50","_13h55",
               "_14h00","_14h05","_14h10","_14h15","_14h20","_14h25","_14h30","_14h35","_14h40","_14h45","_14h50","_14h55",
               "_15h00","_15h05","_15h10","_15h15","_15h20","_15h25","_15h30","_15h35","_15h40","_15h45","_15h50","_15h55",
               "_16h00","_16h05","_16h10","_16h15","_16h20","_16h25","_16h30","_16h35","_16h40","_16h45","_16h50","_16h55",
               "_17h00","_17h05","_17h10","_17h15","_17h20","_17h25","_17h30","_17h35","_17h40","_17h45","_17h50","_17h55",
               "_18h00","_18h05","_18h10","_18h15","_18h20","_18h25","_18h30","_18h35","_18h40","_18h45","_18h50","_18h55",
               "_19h00","_19h05","_19h10","_19h15","_19h20","_19h25","_19h30","_19h35","_19h40","_19h45","_19h50","_19h55",
               "_20h00","_20h05","_20h10","_20h15","_20h20","_20h25","_20h30","_20h35","_20h40","_20h45","_20h50","_20h55",
               "_21h00","_21h05","_21h10","_21h15","_21h20","_21h25","_21h30","_21h35","_21h40","_21h45","_21h50","_21h55",
               "_22h00","_22h05","_22h10","_22h15","_22h20","_22h25","_22h30","_22h35","_22h40","_22h45","_22h50","_22h55",
               "_23h00","_23h05","_23h10","_23h15","_23h20","_23h25","_23h30","_23h35","_23h40","_23h45","_23h50","_23h55")

moisjour <- paste0("_",str_sub(part_date,6,10))
moisjour_aujourdhui <- paste0("_",str_sub(part_date_aujourdhui,6,10))
moisjour <- c(moisjour,moisjour_aujourdhui)

vectheure_df <- crossing(moisjour,vectheure) %>% as.data.frame() %>% 
          mutate(listsuppr=ifelse(moisjour==moisjour_aujourdhui & vectheure=="_06h30","1",NA))
vectheure_df <- fill(vectheure_df,listsuppr) %>% filter(is.na(listsuppr)) %>% select(-listsuppr)
vectheure_df <- unite(vectheure_df,moisjour:vectheure,col="moment",sep="",
                      na.rm = TRUE, remove = TRUE) 
# ainsi obtention vecteur de 366 obs.



# vectheure_df <- vectheure %>% as.data.frame() %>% rename(moment=".")

Vue2 <- Vue1 %>% full_join(vectheure_df, by=c("moment")) %>%
     arrange(moment,Gr_Hotes,name_host,description) %>% 
     mutate(zone=ifelse(is.na(zone),"",zone),
            Gr_Hotes=ifelse(is.na(Gr_Hotes),"",Gr_Hotes),
            name_host=ifelse(is.na(name_host),"",name_host),
            description=ifelse(is.na(description),"",description),
            status_0=ifelse(is.na(status_0),"",status_0)) %>% 
     mutate(statut = case_when(
          status_0=="1_Crit" ~ "1",
          status_0=="2_Warn" ~ "2",
          status_0=="3_Inc"  ~ "3",
          status_0=="4_Ok"   ~ "4",
          TRUE               ~ ".")) %>% 
     select(!status_0)

# on supprime encore les eventuelles obs se rapportant au jour daujourdhui avec 06h30...
Vue2 <- Vue2 %>% filter(!(str_sub(moment,1,6)==moisjour_aujourdhui &
                          str_sub(moment,8,12)=="06h30"))


# Vue <- Vue2 %>% filter(name_host=="ppobservdfle271")
Vue2bis <- Vue2 %>% filter(duplicated(Vue2)) %>% arrange(zone,Gr_Hotes,name_host,description)
# on ne garde que les lignes uniques... afin que les pivots se deroulent bien
Vue2 <- Vue2 %>% unique()

Vue3 <- pivot_wider(Vue2, names_from=moment, values_from = statut) %>%
     as.data.frame() %>% arrange(zone,Gr_Hotes,name_host,description) %>% 
     filter(Gr_Hotes!="") %>% 
     separate(Gr_Hotes, sep = "//", into = c("Gr1", "Gr2"), remove=FALSE)
Vue3[is.na(Vue3)] <- ""  # 373 colonnes a ce stade

vectheure <- Vue2$moment %>% unique()

Vue4 <- Vue3 %>% pivot_longer(cols=vectheure, names_to='heure', values_to='statut') %>%
     as.data.frame() %>% mutate(heure_num=as.integer(str_sub(heure,2,3)))

for(i in 2:nrow(Vue4)){
     if (Vue4$heure_num[i]>=Vue4$heure_num[i-1]){
          if ( Vue4$name_host[i]==Vue4$name_host[i-1] &
               Vue4$description[i]==Vue4$description[i-1] &
               Vue4$statut[i]=="" & Vue4$statut[i-1]!="" & Vue4$statut[i-1]!="4"){
               Vue4$statut[i]=Vue4$statut[i-1]
          }
     }
     if (i %% 5000 == 0){
          print(i)
     }
}
Vue4 <- Vue4 %>% select(-heure_num)

# detection des durees courtes inferieures a 15min
Vue4 <- Vue4 %>% mutate(duree=NA)
for(i in 2:nrow(Vue4)){
     if ( Vue4$name_host[i]==Vue4$name_host[i-1] & Vue4$description[i]==Vue4$description[i-1] ){
          if ( (Vue4$statut[i] %in% c("1","2","3") & Vue4$statut[i-1] %in% c("","4")) |
               (Vue4$statut[i]=="4" & Vue4$statut[i-1]=="") ){
               Vue4$duree[i] <- 0
          }
          if (Vue4$statut[i] %in% c("1","2","3") & Vue4$statut[i-1] %in% c("1","2","3")){
               Vue4$duree[i] <- Vue4$duree[i-1] + 5
          }  
          if (Vue4$statut[i]=="4" & Vue4$statut[i-1] %in% c("1","2","3")){
               Vue4$duree[i] <- Vue4$duree[i-1]
          }
     }
     if (i %% 5000 == 0){
          print(i)
     }
}
Vue_temp <- Vue4 %>% filter(statut=="4") %>% group_by(zone,Gr_Hotes,name_host,description) %>% 
     summarise(nbOK=n(),max_duree=max(duree)) %>% ungroup() %>% as.data.frame()
Vue4 <- Vue4 %>% left_join(Vue_temp, by=c("zone","Gr_Hotes","name_host","description")) %>% 
     arrange(heure,zone,Gr_Hotes,name_host,description) %>% 
     mutate(nbchgt=as.integer(nbchgt))


##########################
# DEBUT PARTIE 1 SEUL CHGT
Vue_1seulOK <- Vue4 %>% filter(nbchgt==1 & statut=="4") %>%
     select(-duree,-nbOK,-max_duree) %>% 
     arrange(heure) %>% select(-nbchgt) %>% 
     mutate(statut = "retour OK") %>% arrange(heure)

Vue_1seulKO <- Vue4 %>% filter(nbchgt==1 & !(statut %in% c("","4"))) %>%
     select(-duree,-nbOK,-max_duree) %>% 
     arrange(heure) %>% arrange(zone,Gr_Hotes,name_host,description) %>% 
     mutate(ident=ifelse(lag(name_host)==name_host & lag(description)==description,1,0)) %>% 
     filter(ident==0 | is.na(ident)) %>% select(-nbchgt,-ident) %>% 
     mutate(statut = case_when(statut=="1" ~ "debut Critical",
                               statut=="2" ~ "debut Warning",
                               statut=="3" ~ "debut Inconnu",
                               TRUE        ~ ".")) %>% arrange(heure)

Vue_1seul <- rbind(Vue_1seulOK,Vue_1seulKO) %>% arrange(heure) %>% relocate(heure) %>% 
     mutate(quartier=ifelse(str_sub(Gr1,1,5)=="-Vlan" & str_length(Gr1)==19,str_sub(Gr1,17,18),"")) %>% 
     mutate(site=ifelse(str_sub(Gr1,1,5)=="-Etab",Gr1,"")) 
Vue_1seul <- unite(Vue_1seul,quartier:site,col="quartier_site",sep="",na.rm = TRUE, remove = TRUE) %>% 
     relocate(quartier_site,.before="Gr_Hotes") %>% select(-Gr_Hotes,-Gr1,-Gr2)
# recodif zone
Vue_1seul <- Vue_1seul %>% 
     mutate(zone=str_sub(zone,4,str_length(zone))) %>%
     mutate(zone=str_replace(zone,"Osny","dc1")) %>% mutate(zone=str_replace(zone,"Auze","dc2")) %>% 
     mutate(zone=str_replace(zone,"SIA","100")) %>% mutate(zone=str_replace(zone,"Int","050")) %>% 
     mutate(zone=str_replace(zone,"Part","Zon025")) %>% mutate(zone=str_replace(zone,"DMZ","Zon000")) %>%
     mutate(zone=str_replace(zone,"Zone","Zon"))
# recodif name_host
Vue_1seul <- Vue_1seul %>% 
     mutate(name_host=ifelse(str_sub(name_host,1,11)=="pd-hlb01-id",
                             str_sub(name_host,4,14),name_host))
# recodif service
Vue_1seul <- Vue_1seul %>% 
     mutate(description=ifelse(str_sub(description,1,13)=="Pool-/Common/",
                               str_sub(description,14,str_length(description)),description)) %>% 
     mutate(description=str_replace(description,"Verification_Presence","Verif_Pres"))
# FIN PARTIE 1 SEUL CHGT
########################

# lignes differentes de Vue_1seulOK
Vue4_SansRetourUniqueOK <- Vue4 %>%
     filter(!(nbchgt==1 & statut=="4"))
Vue4_SansRetourUniqueOK_et_CourteDuree <- Vue4_SansRetourUniqueOK %>%
     filter( (max_duree>15 | is.na(max_duree)) | (nbchgt>10 | is.na(nbchgt)) ) %>% select(-duree)

Vue5 <- pivot_wider(Vue4_SansRetourUniqueOK_et_CourteDuree, names_from=heure, values_from = statut) %>%
     as.data.frame() %>% arrange(zone,Gr_Hotes,name_host,description) %>% 
     mutate(nbOK = as.integer(nbOK), max_duree = as.integer(max_duree)) %>% 
     mutate(nbOK = ifelse(is.na(nbOK),0,nbOK), max_duree = ifelse(is.na(max_duree),0,max_duree))
Vue5[is.na(Vue5)] <- "" # 375 colonnes a ce stade

Vue5 <- Vue5 %>% mutate(nbchgt=as.integer(nbchgt)) %>% 
     filter( (max_duree>15 | is.na(max_duree)) | (nbchgt>10 | is.na(nbchgt)) )
             
      
Vue5 <- Vue5 %>%  
     mutate(quartier=ifelse(str_sub(Gr1,1,5)=="-Vlan" & str_length(Gr1)==19,str_sub(Gr1,17,18),"")) %>% 
     mutate(site=ifelse(str_sub(Gr1,1,5)=="-Etab",Gr1,"")) 
Vue5 <- unite(Vue5,quartier:site,col="quartier_site",sep="",na.rm = TRUE, remove = TRUE) %>% 
     relocate(quartier_site,.before="Gr_Hotes") %>% 
     mutate(quartier_site=ifelse(str_detect(Gr1,"Vlan_DC1_100_PD_STI"),"STI",quartier_site)) %>% 
     #%>% select(-Gr_Hotes,-Gr1,-Gr2)
     mutate(quartier_site=ifelse(str_sub(Gr_Hotes,1,19)=="-RepartiteursCharge","F5",quartier_site)) %>% 
     mutate(quartier_site=ifelse(str_sub(Gr_Hotes,1,11)=="-Messagerie","Messagerie",quartier_site)) %>% 
     mutate(quartier_site=ifelse(str_sub(Gr_Hotes,1,8)=="-Vcenter","VCenter",quartier_site)) %>% 
     mutate(quartier_site=ifelse(str_sub(Gr_Hotes,1,11)=="-ESX_VxRail","ESX_VxRail",quartier_site))
# recodif zone
Vue5 <- Vue5 %>% 
     mutate(zone=str_sub(zone,4,str_length(zone))) %>%
     mutate(zone=str_replace(zone,"Osny","DC1")) %>% mutate(zone=str_replace(zone,"Auze","DC2")) %>% 
     mutate(zone=str_replace(zone,"SIA","100")) %>% mutate(zone=str_replace(zone,"Int","050")) %>% 
     mutate(zone=str_replace(zone,"Part","Zon025")) %>% mutate(zone=str_replace(zone,"DMZ","Zon000")) %>%
     mutate(zone=str_replace(zone,"Zone","Zon"))
# recodif name_host
Vue5 <- Vue5 %>% 
     mutate(name_host=ifelse(str_sub(name_host,1,11)=="pd-hlb01-id",
                             str_sub(name_host,4,14),name_host)) %>% 
     mutate(name_host=ifelse(str_sub(name_host,1,7)=="pd-vmw-",
                        str_sub(name_host,4,str_length(name_host)),name_host))
# recodif service
Vue5 <- Vue5 %>% 
     mutate(description=ifelse(str_sub(description,1,13)=="Pool-/Common/",
                               str_sub(description,14,str_length(description)),description)) %>% 
     mutate(description=str_replace(description,"Verification_Presence","Verif_Pres"))

write.csv2(Vue5,
           paste0(rep_fich,"resum_jour_",part_date,".csv"),
           row.names=FALSE)

#####

write.csv2(Vue_1seul,
           paste0(rep_fich,"resum_jour_retourOK_debutKO_",part_date,".csv"),
           row.names=FALSE)

#####

# Transfert egalement des fichiers dans le repertoire sas vers la z050
rep_sas <- paste0("C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/")

write.csv2(Vue5, 
           paste0(rep_sas,"resum_jour_",part_date,".csv"),
           row.names=FALSE)
write.csv2(Vue_1seul,
           paste0(rep_sas,"resum_jour_retourOK_debutKO_",part_date,".csv"),
           row.names=FALSE)




