
# Conversion JSON du fichier sur 15 jours

# preservation des colonnes...
listCol <- colnames(df_tot) %>% as.data.frame() %>% rename(nomcol=".") %>% 
     filter(nomcol %in% c("dc_zone","quartier","vv","type","indicateur") |
            str_detect(nomcol,"_2")) %>% as.vector() %>% unlist() %>% unname()

df2 <- df_tot %>% select(listCol) %>% filter(vv!="")

# colonnes des dates...
listCol_heure <- listCol[6:length(listCol)]


df2_quota <- df2 %>% filter(indicateur=="quota") %>% 
     select("dc_zone","quartier","vv","type",listCol[length(listCol)]) %>% 
     rename(quota=listCol[length(listCol)])

df2_conso <- df2 %>% filter(indicateur=="conso") %>% 
     pivot_longer(listCol_heure,names_to = "heure") %>%
     mutate(heure="conso") %>% 
     pivot_wider(names_from="heure",values_from = "value", values_fn = list) %>% 
     select(-indicateur)

df2_part <- df2 %>% filter(indicateur=="part") %>% 
     pivot_longer(listCol_heure,names_to = "heure") %>%
     mutate(heure="part") %>% 
     pivot_wider(names_from="heure",values_from = "value", values_fn = list) %>% 
     select(-indicateur)

df2_final <- df2_quota %>% 
     full_join(df2_conso, by=c("dc_zone","quartier","vv","type")) %>% 
     full_join(df2_part, by=c("dc_zone","quartier","vv","type")) %>% 
     relocate(type,.after=quartier)

rm(df2_quota,df2_conso,df2_part)

df2_final <- df2_final %>%
     rename(v1_dc_zone=dc_zone, v2_quartier=quartier, v3_type=type, v4_vv=vv,
            v5_quota=quota, v6_conso=conso, v7_part=part)


# test transformation en json
library(jsonlite)

ASv3_json_15jrs <- toJSON(df2_final)
ASv3_json_15jrs <- prettify(ASv3_json_15jrs)


# suppression de lancien fichier de la veille present dans rep_gen
file.remove(paste0(rep_gen,"ASv3_",type,"_15jrs.json"))
# enregistrement du JSON
write_json(ASv3_json_15jrs,paste0(rep_gen,"ASv3_",type,"_15jrs.json"))

# copie vers le repertoire de transfert sas ver Zone 50...
rep_sas <- "C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/"

file.copy(paste0(rep_gen,"ASv3_",type,"_15jrs.json"),
          paste0(rep_sas,"ASv3_",type,"_15jrs.json"))

# suppression du fichier dans rep_sas apres transfert 
# file.remove(paste0(rep_sas,"ASv3_",type,".json"))
