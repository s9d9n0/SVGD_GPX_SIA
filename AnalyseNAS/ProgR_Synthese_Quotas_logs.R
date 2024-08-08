
# Chargement des packages utiles pour le programme
library(dplyr)
library(stringr)
library(tidyr)
library(readr)

rep_gen <- "C:\\Users\\siar_ycg8l6\\ProgR\\Analyse_Nas\\"
refmois <- "2303"
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
data_QuotaLogs_hr <- data.frame(host_name=c(),dc_zone=c(),quartier=c(),vv=c(),
                                moment=c(),quota=c(),conso=c(),part=c())

for (i in 1:31){
     # lecture du fichier du jour et filtre sur heure retenue comme reference
     fich <- read.csv2(paste0(rep_gen,"Quotas_logs\\","_",refmois,"\\","df_quotas_logs_",refjour[i],".csv")) %>% 
          as.data.frame() %>% 
          filter(str_detect(moment,hr_journee)) 
     
     # concatenation puis tri
     data_QuotaLogs_hr <- rbind(data_QuotaLogs_hr,fich) %>% as.data.frame() %>% 
          arrange(host_name,dc_zone,quartier,vv,moment) #%>% 
    
     # transposition pour les 3 types de mesure : quota, conso, part
     data_QuotaLogs_quota <- data_QuotaLogs_hr %>% select(-conso,-part) %>% 
          pivot_wider(names_from = moment, values_from = quota) %>% 
          mutate(mesure="quota") %>% relocate(mesure,.after="vv")
     
     data_QuotaLogs_conso <- data_QuotaLogs_hr %>% select(-quota,-part) %>% 
          pivot_wider(names_from = moment, values_from = conso) %>% 
          mutate(mesure="conso") %>% relocate(mesure,.after="vv")
     
     data_QuotaLogs_part <- data_QuotaLogs_hr %>% select(-quota,-conso) %>% 
          pivot_wider(names_from = moment, values_from = part) %>% 
          mutate(mesure="part") %>% relocate(mesure,.after="vv")
     
     # concatenation puis tri
     data_QuotaLogs <- rbind(data_QuotaLogs_quota,data_QuotaLogs_conso,data_QuotaLogs_part) %>% 
          arrange(host_name,dc_zone,quartier,vv,mesure)
     rm(data_QuotaLogs_quota,data_QuotaLogs_conso,data_QuotaLogs_part)
     
     # renommage des colonnes
     colnames(data_QuotaLogs) <- str_replace(colnames(data_QuotaLogs),hr_journee,"")
     colnames(data_QuotaLogs) <- str_replace_all(colnames(data_QuotaLogs),"/","")
     # ajout du caractere "m" devant chaque jour
     nomcol <- c()
     for (eltcol in colnames(data_QuotaLogs)){
        if (str_sub(eltcol,1,2)==str_sub(refmois,1,2)){
          nomcol <- append(nomcol,paste0("m",eltcol))
        } else {
          nomcol <- append(nomcol,eltcol)
        }  
     }
     colnames(data_QuotaLogs) <- nomcol
     rm(eltcol,nomcol)
     
}

rm(elt,i)

