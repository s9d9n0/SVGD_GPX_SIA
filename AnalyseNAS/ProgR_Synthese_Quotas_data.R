
# Chargement des packages utiles pour le programme
library(dplyr)
library(stringr)
library(tidyr)
library(readr)

rep_gen <- "C:\\Users\\siar_ycg8l6\\ProgR\\Analyse_Nas\\"
refmois <- "2406"
hr_journee <- "-07h"

##################################################################

vectjour <- c(1:7) %>% as.character()

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
data_QuotaData_hr <- data.frame(host_name=c(),dc_zone=c(),quartier=c(),vv=c(),
                                moment=c(),quota=c(),conso=c(),part=c())

for (i in 1:7){
     # lecture du fichier du jour et filtre sur heure retenue comme reference
     fich <- read.csv2(paste0(rep_gen,"Quotas_data\\","_",refmois,"\\","df_quotas_data_",refjour[i],".csv")) %>% 
          as.data.frame() %>% 
          filter(str_detect(moment,hr_journee)) 
     
     # concatenation puis tri
     data_QuotaData_hr <- rbind(data_QuotaData_hr,fich) %>% as.data.frame() %>% 
          arrange(host_name,dc_zone,quartier,vv,moment) #%>% 
    
     # transposition pour les 3 types de mesure : quota, conso, part
     data_QuotaData_quota <- data_QuotaData_hr %>% select(-conso,-part) %>% 
          pivot_wider(names_from = moment, values_from = quota) %>% 
          mutate(mesure="quota") %>% relocate(mesure,.after="vv")
     
     data_QuotaData_conso <- data_QuotaData_hr %>% select(-quota,-part) %>% 
          pivot_wider(names_from = moment, values_from = conso) %>% 
          mutate(mesure="conso") %>% relocate(mesure,.after="vv")
     
     data_QuotaData_part <- data_QuotaData_hr %>% select(-quota,-conso) %>% 
          pivot_wider(names_from = moment, values_from = part) %>% 
          mutate(mesure="part") %>% relocate(mesure,.after="vv")
     
     # concatenation puis tri
     data_QuotaData <- rbind(data_QuotaData_quota,data_QuotaData_conso,data_QuotaData_part) %>% 
          arrange(host_name,dc_zone,quartier,vv,mesure)
     rm(data_QuotaData_quota,data_QuotaData_conso,data_QuotaData_part)
     
     # renommage des colonnes
     colnames(data_QuotaData) <- str_replace(colnames(data_QuotaData),hr_journee,"")
     colnames(data_QuotaData) <- str_replace_all(colnames(data_QuotaData),"/","")
     # ajout du caractere "m" devant chaque jour
     nomcol <- c()
     for (eltcol in colnames(data_QuotaData)){
        if (str_sub(eltcol,1,2)==str_sub(refmois,1,2)){
          nomcol <- append(nomcol,paste0("m",eltcol))
        } else {
          nomcol <- append(nomcol,eltcol)
        }  
     }
     colnames(data_QuotaData) <- nomcol
     rm(eltcol,nomcol)
     
}

rm(elt,i)

write.csv2(data_QuotaData_hr,
           paste0(rep_gen,"data_QuotaData_",refmois,".csv"),
           row.names=FALSE)


