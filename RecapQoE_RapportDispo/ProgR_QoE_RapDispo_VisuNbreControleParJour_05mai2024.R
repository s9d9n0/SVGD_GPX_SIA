
library(dplyr)
library(stringr)
library(tidyr)


repfichier <- "C:/Users/siar_ycg8l6/ProgR/Centreon_RecapQoE_RapportDispo/"
setwd(paste0(repfichier))

Agreg <- function(nom_date,heure){
     df_out <- read.csv2(paste0(repfichier,"Result_",nom_date,"_",heure,".csv")) %>%
          select(service_description,moment) %>% mutate(jour=str_sub(moment,9,10),unite=1) %>% 
          group_by(service_description,jour) %>% 
          summarise(nb_mesure=sum(unite)) %>% ungroup() %>% as.data.frame() %>% 
          pivot_wider(names_from = jour, values_from = nb_mesure)
     return(df_out)
}

df1 <- Agreg("20240601","00h")

# ou bien directement
# df_out <- read.csv2(paste0(repfichier,"Result_","20230417","_","12h",".csv"))

#######################################
#######################################
# Partie reconstitution donnees du mois
#
# reunion des donnees des fichiers (niveau fin) :
# du 20230104_13h (df_1) avec celui
# du 20230113_23h (df_3)

Result <- rbind(#read.csv2(paste0(repfichier,"Result_","20230121","_","11h",".csv")),
                read.csv2(paste0(repfichier,"Result_","20240601","_","00h",".csv"))) %>% 
          unique() %>% arrange(host_name,service_description,moment)

# Vue <- Result %>% filter(service_description=="QoE-ECMOSS") %>% arrange(moment)

Ctrl <- Result %>% mutate(jour=str_sub(moment,9,10),unite=1) %>% 
     group_by(host_name,service_description,jour) %>% 
     summarise(nb_mesure=sum(unite)) %>% ungroup() %>% as.data.frame() %>% 
     pivot_wider(names_from = jour, values_from = nb_mesure) %>% 
     arrange(service_description)

# write.csv2(Ctrl,paste0(repfichier,"Listing.csv"),row.names = FALSE)
           
# suppression des mesures en doublons... (meme QoE sur 2 serveurs Selenium)
Result2 <- Result %>% filter(!(service_description=="QoE-APIM-RequetesDirect-Sirene_1" &
                               host_name=="PDSPVSELENWD103.ad.insee.intra") &
                                  
                             !(service_description=="QoE-EEC-Sicore" &
                               host_name=="PDSPVSELENWD107.ad.insee.intra") &
      
                             !(service_description=="QoE-KEYCLOACK-DMZ_1" &
                               host_name=="PDSPVSELENWD103.ad.insee.intra") &
                                  
                             !(service_description=="QoE-SICORE-Interne2" &
                               host_name=="PDSPVSELENWD114.ad.insee.intra") &

                             !(str_detect(service_description,"QoETEST"))
                             
                             )


Ctrl2 <- Result2 %>% mutate(jour=str_sub(moment,9,10),unite=1) %>%
     group_by(host_name,service_description,jour) %>%
     summarise(nb_mesure=sum(unite)) %>% ungroup() %>% as.data.frame() %>%
     pivot_wider(names_from = jour, values_from = nb_mesure) %>%
     arrange(service_description)

# Result2 <- Result2 %>% filter(!service_description=="QoE-BRPP_Meta")

write.csv2(Result2,paste0(repfichier,"Result_FINAL",".csv"))


#######################################
#######################################
#######################################
#######################################
#######################################
#######################################
# Partie reconstitution donnees du mois
#
# reunion des donnees des fichiers (niveau agreg) :
# du 20230104_13h (df_1) avec celui
# du 20230113_23h (df_3)

Result_agreg <- rbind(#read.csv2(paste0(repfichier,"Result_agreg_","20230113","_","23h",".csv")),
                      read.csv2(paste0(repfichier,"Result_agreg_","20240601","_","00h",".csv"))) %>% 
                unique() %>% arrange(host_name,service_description,date)

Ctrl <- Result_agreg %>% mutate(unite=1) %>% 
     group_by(host_name,service_description,jour_bis) %>% 
     summarise(nb_mesure=sum(unite)) %>% ungroup() %>% as.data.frame() %>% 
     pivot_wider(names_from = jour_bis, values_from = nb_mesure) %>% 
     arrange(service_description)


# suppression des mesures en doublons... (meme QoE sur 2 serveurs Selenium)
Result_agreg2 <- Result_agreg %>%
                           filter(!(service_description=="QoE-APIM-RequetesDirect-Sirene_1" &
                                    host_name=="PDSPVSELENWD103.ad.insee.intra") &
                                       
                                  !(service_description=="QoE-EEC-Sicore" &
                                    host_name=="PDSPVSELENWD107.ad.insee.intra") &
                                       
                                  !(service_description=="QoE-KEYCLOACK-DMZ_1" &
                                    host_name=="PDSPVSELENWD103.ad.insee.intra") &
                                       
                                  !(service_description=="QoE-SICORE-Interne2" &
                                    host_name=="PDSPVSELENWD114.ad.insee.intra") &

                                  !(str_detect(service_description,"QoETEST"))
                                  
                                    )

Ctrl2 <- Result_agreg2 %>% mutate(unite=1) %>%
     group_by(host_name,service_description,jour_bis) %>%
     summarise(nb_mesure=sum(unite)) %>% ungroup() %>% as.data.frame() %>%
     pivot_wider(names_from = jour_bis, values_from = nb_mesure) %>%
     arrange(service_description)

# Result_agreg2 <- Result_agreg2 %>% filter(!service_description=="QoE-BRPP_Meta")

write.csv2(Result_agreg2,paste0(repfichier,"Result_agreg_FINAL",".csv"))

# Vue <- Result_agreg2 %>% filter(service_description=="QoE-BRPP_Meta" & disponibilite!=100)
