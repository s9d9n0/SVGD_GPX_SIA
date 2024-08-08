
# Conversion JSON

# preservation des colonnes...
listCol <- colnames(ensemble) %>% as.data.frame() %>%
     rename(nomcol=".") %>% 
     filter(nomcol %in% c("dc_zone","quartier","vv","type","indicateur") |
            str_detect(nomcol,"/")) %>% as.vector() %>% unlist() %>% unname()

ens2 <- ensemble %>% select(listCol) %>% filter(vv!="")

# renommage des colonnes...
listCol <- colnames(ens2) %>% as.data.frame() %>%
     rename(nomcol=".") %>% 
     filter(nomcol %in% c("dc_zone","quartier","vv","type","indicateur") |
            str_detect(nomcol,"/")) %>% 
     mutate(heure=str_sub(nomcol,str_length(nomcol),str_length(nomcol))) %>% 
     mutate(nomcol=ifelse(heure=="h",
                          str_sub(nomcol,str_length(nomcol)-2,str_length(nomcol)),
                          nomcol)) %>% select(nomcol) %>%
     as.vector() %>% unlist() %>% unname()

colnames(ens2) <- listCol

listCol_heure <- listCol[6:length(listCol)]

###

ens2_quota <- ens2 %>% filter(indicateur=="quota") %>% 
     select("dc_zone","quartier","vv","type",listCol[length(listCol)]) %>% 
     rename(quota=listCol[length(listCol)])

# ens2_conso <- ens2 %>% filter(indicateur=="conso") %>% 
#      unite(all_of(listCol_heure),
#            col="conso",sep=",") %>% 
#      mutate(conso=paste0("[",conso,"]")) %>%
#      select(-indicateur)
ens2_conso <- ens2 %>% filter(indicateur=="conso") %>% 
     pivot_longer(listCol_heure,names_to = "heure") %>%
     mutate(heure="conso") %>% 
     pivot_wider(names_from="heure",values_from = "value") %>% 
     select(-indicateur)


# ens2_part <- ens2 %>% filter(indicateur=="part") %>% 
#      unite(all_of(listCol_heure),
#            col="part",sep=",") %>% 
#      mutate(part=paste0("[",part,"]")) %>%
#      select(-indicateur)
ens2_part <- ens2 %>% filter(indicateur=="part") %>% 
     pivot_longer(listCol_heure,names_to = "heure") %>%
     mutate(heure="part") %>% 
     pivot_wider(names_from="heure",values_from = "value") %>% 
     select(-indicateur)


ens2_final <- ens2_quota %>% 
     full_join(ens2_conso, by=c("dc_zone","quartier","vv","type")) %>% 
     full_join(ens2_part, by=c("dc_zone","quartier","vv","type")) %>% 
     mutate(date=str_sub(vect_jour,1,10)) %>% 
     relocate(date) %>% relocate(type,.after=quartier)

rm(ens2_quota,ens2_conso,ens2_part)


ens2_final[is.na(ens2_final)] <- 0

ens2_final <- ens2_final %>%
     rename(v1_date=date, v2_dc_zone=dc_zone, v3_quartier=quartier, v4_type=type, v5_vv=vv,
            v6_quota=quota, v7_conso=conso, v8_part=part)


# test transformation en json
library(jsonlite)

ASv3_json <- toJSON(ens2_final)
ASv3_json <- prettify(ASv3_json)

# suppression de lancien fichier de la veille present dans rep_gen
file.remove(paste0(rep_gen,"ASv3_",type,".json"))
# enregistrement du JSON
write_json(ASv3_json,paste0(rep_gen,"ASv3_",type,".json"))

# copie vers le repertoire de transfert sas ver Zone 50...
rep_sas <- "C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/"

file.copy(paste0(rep_gen,"ASv3_",type,".json"),
          paste0(rep_sas,"ASv3_",type,".json"))

# suppression du fichier dans rep_sas apres transfert 
# file.remove(paste0(rep_sas,"ASv3_",type,".json"))


