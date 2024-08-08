
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

date_jour_0 <- Sys.time()
date_jour_5 <- Sys.time() - dminutes(5)

part_date_0 <- str_sub(as.character(date_jour_0),1,10)
part_date_5 <- str_sub(as.character(date_jour_5),1,10)

# part_mois_0 <- as.character(month(date_jour_0))
# part_jour_0 <- as.character(day(date_jour_0))
part_heure_0 <- as.character(hour(date_jour_0))
part_minut_0 <- as.character(minute(date_jour_0))

# part_mois_5 <- as.character(month(date_jour_5))
# part_jour_5 <- as.character(day(date_jour_5))
part_heure_5 <- as.character(hour(date_jour_5))
part_minut_5 <- as.character(minute(date_jour_5))

if (str_length(part_heure_0)==1){part_heure_0 <- paste0("0",part_heure_0)}
if (str_length(part_minut_0)==1){part_minut_0 <- paste0("0",part_minut_0)}
lib_date_jr_0 <- paste0(part_date_0,"_",part_heure_0,"h",part_minut_0)

if (str_length(part_heure_5)==1){part_heure_5 <- paste0("0",part_heure_5)}
if (str_length(part_minut_5)==1){part_minut_5 <- paste0("0",part_minut_5)}
lib_date_jr_5 <- paste0(part_date_5,"_",part_heure_5,"h",part_minut_5)

rm(part_date_0,part_heure_0,part_minut_0,
   part_date_5,part_heure_5,part_minut_5)

# arrondi par intervalle de 5 minutes afin de retrouver les fichiers
if (str_sub(lib_date_jr_0,16,16) %in% c("5","6","7","8","9")){
     lib_date_jr_0 <- paste0(str_sub(lib_date_jr_0,1,15),"5")
} else {
     lib_date_jr_0 <- paste0(str_sub(lib_date_jr_0,1,15),"0")
}

if (str_sub(lib_date_jr_5,16,16) %in% c("5","6","7","8","9")){
     lib_date_jr_5 <- paste0(str_sub(lib_date_jr_5,1,15),"5")
} else {
     lib_date_jr_5 <- paste0(str_sub(lib_date_jr_5,1,15),"0")
}


########################################################
########################################################


# definition du ou des repertoires
date_0 <- str_sub(str_replace_all(date_jour_0,"-",""),1,8)
date_5 <- str_sub(str_replace_all(date_jour_5,"-",""),1,8)
chem_0 <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",date_0)
chem_5 <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",date_5)

# detection des 2 fichiers csv necessaires 
df_0 <- list.files(chem_0) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich) %>% 
     filter(str_detect(liste_fich,"df_datareq") &
            str_detect(liste_fich,str_sub(lib_date_jr_0,12,16))) 
print(df_0)
df_0 <- read.csv2(paste0(chem_0,"/",df_0))
                  
df_5 <- list.files(chem_5) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich) %>% 
     filter(str_detect(liste_fich,"df_datareq") &
            str_detect(liste_fich,str_sub(lib_date_jr_5,12,16)))
print(df_5)
df_5 <- read.csv2(paste0(chem_5,"/",df_5))


########################################################
########################################################

# debut de la phase de comparaison sur cet intervalle de 5 minutes

df_5_gr <- df_5 %>% group_by(zone,Gr_Hotes,last_hard_state) %>% 
     summarize(eff=n()) %>% ungroup() %>% as.data.frame() %>% 
     rename(eff_5=eff)

df_0_gr <- df_0 %>% group_by(zone,Gr_Hotes,last_hard_state) %>% 
     summarize(eff=n()) %>% ungroup() %>% as.data.frame() %>% 
     rename(eff_0=eff)

# reglage ici de la finesse de detection !!
regroup <- df_5_gr %>% 
     full_join(df_0_gr, by=c("zone","Gr_Hotes","last_hard_state")) %>% 
     mutate(Gr_Hotes=ifelse(is.na(Gr_Hotes),"SANS HostGroup",Gr_Hotes),
            eff_5=ifelse(is.na(eff_5),0,eff_5),
            eff_0=ifelse(is.na(eff_0),0,eff_0),
            diff=eff_0-eff_5) %>% filter(diff!=0) %>% 
     filter(Gr_Hotes!="SANS HostGroup") %>% 
     mutate(last_hard_state = case_when(
               last_hard_state==0 ~ "4_Ok",   last_hard_state==1 ~ "2_Warn",
               last_hard_state==2 ~ "1_Crit", last_hard_state==3 ~ "3_Inc",
               TRUE ~ "5_Autre")) %>%
     rename(diff_sur_statut=last_hard_state) %>% 
     mutate(moment=lib_date_jr_0) %>% relocate(moment) %>% 
     arrange(desc(diff),diff_sur_statut,Gr_Hotes)


########################################################
########################################################


# retour a un niveau fin Hote x Service
list_group <- as.list(regroup$Gr_Hotes)

Visu_df_5 <- df_5 %>% filter(Gr_Hotes %in% list_group) %>%
     select(zone,Gr_Hotes,name_host,description,last_hard_state) %>% 
     rename(status_5=last_hard_state)

Visu_df_0 <- df_0 %>% filter(Gr_Hotes %in% list_group) %>%
     select(zone,Gr_Hotes,name_host,description,last_hard_state) %>% 
     rename(status_0=last_hard_state)

delta_group <- Visu_df_5 %>% 
     full_join(Visu_df_0, by=c("zone","Gr_Hotes","name_host","description")) %>% 
     mutate(status_5=ifelse(is.na(status_5),0,status_5),
            status_0=ifelse(is.na(status_0),0,status_0),
            chgt=ifelse(status_0!=status_5,"chgt","")) %>%
     mutate(status_5 = case_when(
          status_5==0 ~ "4_Ok",   status_5==1 ~ "2_Warn",
          status_5==2 ~ "1_Crit", status_5==3 ~ "3_Inc",
          TRUE ~ "5_Autre")) %>%
     mutate(status_0 = case_when(
          status_0==0 ~ "4_Ok",   status_0==1 ~ "2_Warn",
          status_0==2 ~ "1_Crit", status_0==3 ~ "3_Inc",
          TRUE ~ "5_Autre")) %>%
     arrange(Gr_Hotes,zone,name_host,description)


# recuperation de la difference au niveau Gr_Hotes
delta_group_final <- regroup %>% 
     inner_join(delta_group, by=c("zone","Gr_Hotes")) %>% 
     arrange(desc(diff),diff_sur_statut,Gr_Hotes,zone,name_host,description)
             

########################################################
########################################################

# sauvegarde des resultats dans un repertoire Windows

# Creation dun repertoire avec la date du jour
folder <- paste0("C:/Users/siar_ycg8l6/ProgR/CompteurAlertes_5min/Fichiers_date_",format(Sys.time(),'%Y%m%d'),"_delta_5min")
if (file.exists(folder)) {
     cat("The folder already exists")
} else {
     dir.create(folder, showWarnings = TRUE, recursive = FALSE, mode = "0777")
}

write.csv2(regroup, 
           paste0(folder,"/df_regroup_",str_sub(lib_date_jr_0,3,16),".csv"),
           row.names=FALSE)
write.csv2(delta_group_final,
           paste0(folder,"/df_delta_group_",str_sub(lib_date_jr_0,3,16),".csv"),
           row.names=FALSE)


# Transfert egalement des fichiers dans le repertoire sas vers la z050
rep_sas <- paste0("C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/")

write.csv2(regroup, 
           paste0(rep_sas,"/df_regroup_",str_sub(lib_date_jr_0,3,16),".csv"),
           row.names=FALSE)
write.csv2(delta_group_final,
           paste0(rep_sas,"/df_delta_group_",str_sub(lib_date_jr_0,3,16),".csv"),
           row.names=FALSE)
