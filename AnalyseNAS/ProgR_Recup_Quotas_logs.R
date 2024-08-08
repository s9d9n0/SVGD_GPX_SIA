##################################################################################
##################################################################################

# PARTIE REQUETE DE LA BDD CENTREON

# Connexion base Osny 100% 
baseMdb <- dbConnect(MariaDB(),user="Gepex_lecture", password="Reporting",
                     dbname="centreon_storage", host="XX.XX.XX.XX", port="3306")

##################################################################################

# recuperation des datas sur les 24h de la journee consideree
Req_Logs <- dbGetQuery(baseMdb,
                       paste("SELECT i.host_name, m.metric_name,
                                v.ctime, v.value, m.unit_name
                         FROM data_bin v
                              INNER JOIN metrics m on v.id_metric = m.metric_id
                              INNER JOIN index_data i on m.index_id = i.id
                         WHERE i.host_name LIKE ('%pd-nas1%') AND
                               i.service_description LIKE ('%Quotas_Logs%') AND
                               v.ctime >= UNIX_TIMESTAMP('",date_jour_ref0,"') AND
                               v.ctime <= UNIX_TIMESTAMP('",date_jour_ref1,"')
                         ORDER BY v.ctime DESC
                        "))

# Deconnexion
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())

##################################################################################
##################################################################################

options(scipen=100)
Requete <- Req_Logs %>% mutate(POSIXct=ymd_hms(as_datetime(ctime))+dhours(decalage_heure)) %>% 
     select(-ctime) %>% 
     # converson en GB
     mutate(mesure=ifelse(unit_name=="B",round(value*9.313225746154785e-10,2),round(value,2))) %>% 
     select(-value) %>% 
     filter(!str_detect(metric_name,"^vserver:pd_aus"))
     
df_quotas_logs <- Requete %>% 
     separate(metric_name,sep="~",into=c("part01","part02","part03")) %>% 
     # filter(!is.na(part02) && !is.na(part03)) %>% 
     separate(part03,sep="#",into=c("part03","part04")) %>% 
     # select(-service_description,-part02,-unit_name,-warn,-crit) %>% 
     mutate(part01=str_replace(part01,"vserver:",""),
            part03=str_replace(part03,"qtree:",""),
            part04=str_replace(part04,"quota.space.","")) %>% 
     separate(part01,sep="_",into=c("part011","part012","part013","part014","part015")) %>%
     select(-part012,-part02,-unit_name) %>% 
     rename(env=part011, quartier=part013, dc=part014, zone=part015, vv=part03) %>% 
     mutate(env=str_to_upper(env), quartier=str_to_upper(quartier),
            dc_zone=paste0(str_to_upper(dc),"_",zone)) %>% select(-dc,-zone) %>%  
     relocate(dc_zone,.before=quartier) %>% unique() %>%
     pivot_wider(names_from = part04, values_from = mesure) %>% as.data.frame() %>% 
     rename(usage=usage.bytes,
            free=free.bytes,
            prct=usage.percentage)

df_quotas_logs <- df_quotas_logs %>%
     mutate(quota=round(usage+free,0),
            conso=round(usage,1),
            part=round(usage/(usage+free)*100,1)) %>% 
     select(-env,-usage,-free,-prct) %>% arrange(host_name, dc_zone, quartier, vv, POSIXct) %>% 
     mutate(an=year(POSIXct),mois=month(POSIXct),jour=day(POSIXct),
            heure=hour(POSIXct),min=minute(POSIXct)) %>% 
     mutate(mois=ifelse(mois<10,paste0("0",mois),mois),     jour=ifelse(jour<10,paste0("0",jour),jour), 
            heure=ifelse(heure<10,paste0("0",heure),heure), demiheure=ifelse(min>=30,"30","00")) %>% 
     mutate(moment=paste0(an,"/",mois,"/",jour,"-",heure,"h")) %>% 
     mutate(moment=str_sub(moment,3,str_length(moment))) %>% 
     relocate(moment,.before=POSIXct) %>% 
     select(-POSIXct,-an:-demiheure) %>%  
     mutate(chgt_heure=ifelse(moment!=lag(moment) | row_number()==1
                              ,1,0)) %>% filter(chgt_heure==1) %>% 
     select(-chgt_heure)

gc(reset=TRUE)

df_quotas_logs_indic_quota <- df_quotas_logs %>% 
     mutate(type="logs",
            an_mois_jour=str_sub(moment,1,8),
            heure=str_sub(moment,10,12),
            indicateur="quota") %>% 
     select(-moment,-conso,-part) %>% 
     pivot_wider(names_from = c(an_mois_jour,heure), values_from = quota) %>% as.data.frame()

df_quotas_logs_indic_conso <- df_quotas_logs %>% 
     mutate(type="logs",
            an_mois_jour=str_sub(moment,1,8),
            heure=str_sub(moment,10,12),
            indicateur="conso") %>% 
     select(-moment,-quota,-part) %>% 
     pivot_wider(names_from = c(an_mois_jour,heure), values_from = conso) %>% as.data.frame()

df_quotas_logs_indic_part <- df_quotas_logs %>% 
     mutate(type="logs",
            an_mois_jour=str_sub(moment,1,8),
            heure=str_sub(moment,10,12),
            indicateur="part") %>% 
     select(-moment,-quota,-conso) %>% 
     pivot_wider(names_from = c(an_mois_jour,heure), values_from = part) %>% as.data.frame()

ensemble <- rbind(df_quotas_logs_indic_conso,
                  df_quotas_logs_indic_quota,
                  df_quotas_logs_indic_part) %>% 
     arrange(host_name,dc_zone,quartier,vv)

rm(df_quotas_logs_indic_conso,
   df_quotas_logs_indic_quota,
   df_quotas_logs_indic_part)

# repertoire <- "C:/Users/siar_ycg8l6/ProgR/Analyse_Nas/Quotas_logs/"
repertoire <- paste0(rep_gen,"Quotas_logs/")
write.csv2(ensemble,
           paste0(repertoire,"df_quotas_logs_",partie_an,partie_mois,partie_jour,".csv"),
           row.names=FALSE)


## partie JSON
source(paste0(rep_gen,"ProgR_ConvertJSON.R"))


