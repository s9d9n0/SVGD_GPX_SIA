
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)
library(readODS)

partie_mois <- "_2024_05"

rep_fich <- paste0("C:/Users/siar_ycg8l6/ProgR/HistoCentreon_User/",partie_mois,"/")
setwd(rep_fich)
partie_mois2 <- paste0(str_sub(partie_mois,1,5),"-",str_sub(partie_mois,7,8))

VisuMois <- read_ods(paste0(rep_fich,"CentreonSIA_user",partie_mois2,".ods"), sheet="Visu")

###

transfo_heureminute <- function(var){
  jour <- var %/% 1440
  jour_reste=var%%1440
  heure=jour_reste%/%60
  minute=jour_reste%%60
  if (jour<10)  { jour   <- paste0("0",jour)  }  else { jour   <- paste0("",jour) }
  if (heure<10) { heure  <- paste0("0",heure) }  else { heure  <- paste0("",heure) }
  if (minute<10){ minute <- paste0("0",minute) } else { minute <- paste0("",minute) }
  resultat <- paste0(jour,"j ",heure,"h ",minute,"m")
  return (resultat)
}

# transfo_heureminute(1440)

###

Agreg_Jour <- VisuMois %>% select(jour,contact_name,duree_min) %>% unique() %>%
  group_by(jour) %>% summarise(duree_mois=sum(duree_min)) %>% ungroup() %>% as.data.frame()

Agreg_Contact <- VisuMois %>% select(jour,contact_name,duree_min) %>% unique() %>%
  group_by(contact_name) %>% summarise(duree_mois=sum(duree_min)) %>% ungroup() %>% as.data.frame() %>% 
  arrange(desc(duree_mois)) %>% 
  mutate(site=case_when(
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / G4",
          contact_name %in% c("XXX","admin1",
                              "XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / Gpxx",
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / RIAP",
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR",) ~ "XXX/ Integ",
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / Obs",
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / Res", 
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / Syst",
          contact_name %in% c("XXX_SIAR") ~ "XXX / Kube",         
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / PTL",
          contact_name %in% c("XXX_SIAR") ~ "XXX / DB",
          contact_name %in% c("XXX_SIAR") ~ "XXX / Si@moi", 
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "XXX / Cugid",
          contact_name %in% c("XXX_SIAR") ~ "XXX / SOC",
          contact_name %in% c("XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR",
                              "XXX_SIAR") ~ "Ext. XXX",
          TRUE ~ "Autre")) %>% relocate(site)

Agreg_Equipe <- Agreg_Contact %>% 
  group_by(site) %>% summarise(duree_mois=sum(duree_mois)) %>% ungroup() %>% as.data.frame() %>% 
  arrange(desc(duree_mois))

Agreg_Jour <- Agreg_Jour %>% mutate(duree_label=lapply(duree_mois,transfo_heureminute))
Agreg_Equipe <- Agreg_Equipe %>% mutate(duree_label=lapply(duree_mois,transfo_heureminute))
Agreg_Contact <- Agreg_Contact %>% mutate(duree_label=lapply(duree_mois,transfo_heureminute))


write_ods(Agreg_Jour,paste0(rep_fich,"AGREG_centreonSIA",partie_mois2,".ods"), 
          sheet="AgregJour", append = TRUE)
write_ods(Agreg_Equipe,paste0(rep_fich,"AGREG_centreonSIA",partie_mois2,".ods"),
          sheet="AgregEquipe", append = TRUE)
write_ods(Agreg_Contact,paste0(rep_fich,"AGREG_centreonSIA",partie_mois2,".ods"),
          sheet="AgregAgent", append = TRUE)










