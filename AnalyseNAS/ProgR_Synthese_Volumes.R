
# Chargement des packages utiles pour le programme
library(dplyr)
library(stringr)
library(tidyr)
library(readr)

rep_gen <- "C:\\Users\\siar_ycg8l6\\ProgR\\Analyse_Nas\\"
refmois <- "2305"
hr_journee <- "-07h"

##################################################################

vectjour <- c(1:31) %>% as.character()

refjour <- c()
for (elt in vectjour){
     if (str_length(elt)==1){ 
          refjour <- append(refjour,paste0(refmois,"0",elt))
     } else {
          refjour <- append(refjour,paste0(refmois,elt))
     }
}
print(refjour)

# initialisation dataframe
dataVol_hr <- data.frame(host_name=c(),env=c(),dc_zone=c(),quartier=c(),typevol=c(),
                         moment=c(),quota=c(),conso=c(),part=c())

for (i in 1:31){
     # lecture du fichier du jour et filtre sur heure retenue comme reference
     fich <- read.csv2(paste0(rep_gen,"Volumes\\","_",refmois,"\\","df_Volumes_",refjour[i],".csv")) %>% 
          as.data.frame() %>% 
          filter(str_detect(moment,hr_journee)) 
     
     # concatenation puis tri
     dataVol_hr <- rbind(dataVol_hr,fich) %>% as.data.frame() %>% 
          arrange(host_name,env,dc_zone,quartier,typevol,moment) #%>% 
    
     # transposition pour les 3 types de mesure : quota, conso, part
     dataVol_quota <- dataVol_hr %>% select(-conso,-part) %>% 
          pivot_wider(names_from = moment, values_from = quota) %>% 
          mutate(mesure="quota") %>% relocate(mesure,.after="typevol")
     
     dataVol_conso <- dataVol_hr %>% select(-quota,-part) %>% 
          pivot_wider(names_from = moment, values_from = conso) %>% 
          mutate(mesure="conso") %>% relocate(mesure,.after="typevol")
     
     dataVol_part <- dataVol_hr %>% select(-quota,-conso) %>% 
          pivot_wider(names_from = moment, values_from = part) %>% 
          mutate(mesure="part") %>% relocate(mesure,.after="typevol")
     
     # concatenation puis tri
     dataVol <- rbind(dataVol_quota,dataVol_conso,dataVol_part) %>% 
          arrange(host_name,env,dc_zone,quartier,typevol,mesure)
     rm(dataVol_quota,dataVol_conso,dataVol_part)
     
     # renommage des colonnes
     colnames(dataVol) <- str_replace(colnames(dataVol),hr_journee,"")
     colnames(dataVol) <- str_replace_all(colnames(dataVol),"/","")
     # ajout du caractere "m" devant chaque jour
     nomcol <- c()
     for (eltcol in colnames(dataVol)){
        if (str_sub(eltcol,1,2)==str_sub(refmois,1,2)){
          nomcol <- append(nomcol,paste0("m",eltcol))
        } else {
          nomcol <- append(nomcol,eltcol)
        }  
     }
     colnames(dataVol) <- nomcol
     rm(eltcol,nomcol)
     
}

rm(elt,i)

# write.csv2(dataVol_hr,
#            paste0(rep_gen,"data_Vol_",refmois,".csv"),
#            row.names=FALSE)

