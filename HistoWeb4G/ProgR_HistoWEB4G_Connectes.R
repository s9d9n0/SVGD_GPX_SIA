
## Chargement des packages utilises dans le programme ----
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

date_jour <- Sys.time()
date_jour <- Sys.Date()

# decalage egal a 2 en heure d'ete et a 1 en heure d'hiver
decalage_heure <- 2

part_date <- as.character(Sys.Date())
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))
part_seconde <- as.character(round(second(date_jour),0))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
if (str_length(part_seconde)==1){part_seconde <- paste0("0",part_seconde)}
lib_date_jr <- paste0(part_date,"_",part_heure,"h",part_minute,"m",part_seconde,"s")
moment <- paste0(part_date,"_",part_heure,"h",part_minute)

rm(part_date,part_heure,part_minute,part_seconde)


datej_deb <- date_jour - ddays(15)
datej_fin <- date_jour - ddays(0)

# semaine courante
datej_semcrt_fin <- paste0(date_jour - ddays(1))
datej_semcrt_deb <- paste0(date_jour - ddays(7))
# semaine de reference
datej_semref_fin <- paste0(date_jour - ddays(8))
datej_semref_deb <- paste0(date_jour - ddays(14))

J08_J01 <- paste0(str_sub(paste0(date_jour - ddays(8)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(1)),6,10))
J09_J02 <- paste0(str_sub(paste0(date_jour - ddays(9)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(2)),6,10))
J10_J03 <- paste0(str_sub(paste0(date_jour - ddays(10)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(3)),6,10))
J11_J04 <- paste0(str_sub(paste0(date_jour - ddays(11)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(4)),6,10))
J12_J05 <- paste0(str_sub(paste0(date_jour - ddays(12)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(5)),6,10))
J13_J06 <- paste0(str_sub(paste0(date_jour - ddays(13)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(6)),6,10))
J14_J07 <- paste0(str_sub(paste0(date_jour - ddays(14)),6,10)," // ",
                  str_sub(paste0(date_jour - ddays(7)),6,10))

##########################################################

##################################################
##                                              ##
## 1/ PARTIE REQUETE DE LA BDD CENTREON         ##
##                                              ##
################################################## ----
##
## Connexion a la nouvelle base Centreon EDIFICE Osny 100 ----
## maitre xx.xx.xx.xx ; esclave xx.xx.xx.xx

##
## Connexion a la base Centreon ----
##

baseMdb <- dbConnect(MariaDB(), user="Gepex_lecture", password="Reporting", dbname="centreon_storage", 
                     host="xx.xx.xx.xx", port="3306")


##################################################################

## REQUETE LISTING DES METRIQUES DISPONIBLES ##

Req <- dbGetQuery(baseMdb,
                paste("SELECT v.id_metric AS v_val_id, v.ctime, v.value,
                              m.metric_id AS m_val_id, m.index_id AS m_id, m.metric_name,
                              m.unit_name, m.warn, m.crit,
                              i.id AS i_id, i.host_id, i.host_name, i.service_id, i.service_description
                       FROM data_bin v
                            inner join metrics m on v.id_metric = m.metric_id
                            inner join index_data i on m.index_id = i.id
                       WHERE (i.host_name LIKE ('%pd-hlb01-id161-mgt%') AND
                              i.service_description LIKE ('%Pool-/Common/pool_pdweb4gfolht74_dmz%') AND
                              m.metric_name LIKE ('%pool.connections.server.count%') AND
                              ctime >= UNIX_TIMESTAMP('",datej_deb,"') AND
                              ctime <= UNIX_TIMESTAMP('",datej_fin,"'))
                       "))

##################################################################

##
## Deconnexion de la base Centreon ----
##
dbDisconnect(baseMdb)
dbUnloadDriver(MariaDB())
##################################################################


Req_etude <- Req %>% mutate(moment = as.POSIXct(ctime,origin="1970-01-01")) %>%
     mutate(lg=row_number(),
            an=year(moment),mois=month(moment),jour=day(moment),
            heur=hour(moment),min=minute(moment)) %>% 
     mutate(an=paste0(an),
            mois=ifelse(mois<10,paste0("0",mois),mois),
            jour=ifelse(jour<10,paste0("0",jour),jour),
            heur=ifelse(heur<10,paste0("0",heur),heur),
            min=ifelse(min<10,paste0("0",min),min),
            jour_lib=paste0(an,"-",mois,"-",jour)) %>% 
     mutate(heur_app=ifelse(str_sub(min,2,2) %in% c("0","1","2","3","4"),
                           paste0(heur,":",str_sub(min,1,1),"2:30"),
                           paste0(heur,":",str_sub(min,1,1),"7:30"))) %>% 
     select(-heur,-min) %>% 
     mutate(jour_app=case_when(
          (str_sub(jour_lib,6,10)==str_sub(J08_J01,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J08_J01,1,5)) ~ J08_J01,
          (str_sub(jour_lib,6,10)==str_sub(J09_J02,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J09_J02,1,5)) ~ J09_J02,
          (str_sub(jour_lib,6,10)==str_sub(J10_J03,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J10_J03,1,5)) ~ J10_J03,
          (str_sub(jour_lib,6,10)==str_sub(J11_J04,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J11_J04,1,5)) ~ J11_J04,
          (str_sub(jour_lib,6,10)==str_sub(J12_J05,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J12_J05,1,5)) ~ J12_J05,
          (str_sub(jour_lib,6,10)==str_sub(J13_J06,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J13_J06,1,5)) ~ J13_J06,
          (str_sub(jour_lib,6,10)==str_sub(J14_J07,10,14) | 
           str_sub(jour_lib,6,10)==str_sub(J14_J07,1,5)) ~ J14_J07,
          TRUE ~ ""
          )) %>% 
     select(lg,ctime,an,mois,jour,jour_app,heur_app,jour_lib,moment,value) %>% arrange(desc(ctime)) %>% 
     mutate(mm_1h=(lag(value,6)+lag(value,5)+lag(value,4)+
                   lag(value,3)+lag(value,2)+lag(value,1)+
                   value+
                   lead(value,1)+lead(value,2)+lead(value,3)+
                   lead(value,4)+lead(value,5)+lead(value,6))/13) %>% 
     mutate(mm_1h=round(mm_1h))

#filtre des observations sur la semaine de reference
lg_deb_semref <- Req_etude %>% filter(jour_lib==datej_semref_deb) %>% 
     mutate(min_lg=min(lg)) %>%  filter(lg==min_lg) %>% select(lg) %>% as.integer()
lg_fin_semref <- Req_etude %>% filter(jour_lib==datej_semref_fin) %>% 
     mutate(max_lg=max(lg)) %>%  filter(lg==max_lg) %>% select(lg) %>% as.integer()                                

Req_etude_semref <- Req_etude %>% filter(lg>=lg_deb_semref & lg<=lg_fin_semref)
Vue <- Req_etude_semref %>% group_by(jour_lib) %>% summarise(nbobs = n()) %>% ungroup() %>% as.data.frame()
                                
#filtre des observations sur la semaine courante                            
lg_deb_semcrt <- Req_etude %>% filter(jour_lib==datej_semcrt_deb) %>% 
     mutate(min_lg=min(lg)) %>%  filter(lg==min_lg) %>% select(lg) %>% as.integer()
lg_fin_semcrt <- Req_etude %>% filter(jour_lib==datej_semcrt_fin) %>% 
     mutate(max_lg=max(lg)) %>%  filter(lg==max_lg) %>% select(lg) %>% as.integer()                                

Req_etude_semcrt <- Req_etude %>% filter(lg>=lg_deb_semcrt & lg<=lg_fin_semcrt)
Vue <- Req_etude_semcrt %>% group_by(jour_lib) %>% summarise(nbobs = n()) %>% ungroup() %>% as.data.frame()

####

Req_etude_semref <- Req_etude_semref %>% select(jour_app,heur_app,value,mm_1h) %>% 
     arrange(jour_app,heur_app) %>% rename(value_ref=value,mm_1h_ref=mm_1h) %>% 
     mutate(sem="sem. reference") %>% relocate(sem)

Req_etude_semcrt <- Req_etude_semcrt %>% select(jour_app,heur_app,value,mm_1h) %>% 
     arrange(jour_app,heur_app) %>% rename(value_crt=value,mm_1h_crt=mm_1h) %>% 
     mutate(sem="sem. courante") %>% relocate(sem)


Req_Fusion <- Req_etude_semref %>% full_join(Req_etude_semcrt, by=c("jour_app","heur_app")) %>% 
     arrange(jour_app,heur_app)


####

###############################
# Fonction de relabelling des x
fct_modif_label_x <- function(df){
     ########################################
     # DEBUT Creation du vecteur moment label
     moment_vect <- df %>% select(moment,moment_label) %>%
          mutate(ligne=row_number()) %>% 
          select(ligne,moment,moment_label)
     
     virgule <- c(',')
     for (i in 1:(nrow(moment_vect)-2)){
          virgule <- append(virgule,',')
     }
     virgule <- virgule %>% as.data.frame() %>%
          rename(moment_label = ".") %>% 
          mutate(ligne=row_number(),moment="") %>% select(ligne,moment,moment_label) 
     
     moment_vect <- rbind(moment_vect,virgule) %>% arrange(ligne)
     
     char <- c()
     for (i in 1:nrow(moment_vect)){
          char <- append(char,moment_vect$moment_label[i])
          cat(moment_vect$moment_label[i],"\n")
     }
     names(char) <- moment_vect$moment
     
     return (char)
     # structure attendue du char...
     # char <- c('23-06-28_00h00' = '23-06-28_00h00',
     #           ',',
     #           '23-06-28_00h05' = '_',
     #           ',',
     #           '23-06-28_00h10' = '23-06-28_00h10',
     #           ',',
     #           '23-06-28_00h15' = '_')
     
     # FIN  Creation du vecteur moment label
     ########################################    
}



Req_Fusion_graph <- Req_Fusion %>% 
     mutate(lg=row_number(),
            moment=paste0(jour_app," - ",heur_app)) %>% 
     mutate(moment_label=ifelse((lg==1 | jour_app!=lag(jour_app)),moment,""),
            moment_label=ifelse((moment_label=="" & (lg==1 | str_sub(heur_app,1,2)!=lag(str_sub(heur_app,1,2)))),
                                 str_sub(moment,18,22),moment_label),
            moment_label=ifelse((str_length(moment_label)==5 & 
                                 str_sub(moment_label,1,2) %in% c("01","02","03",
                                                                  "05","06","07",
                                                                  "09","10","11",
                                                                  "13","14","15",
                                                                  "17","18","19",
                                                                  "21","22","23")),
                                "",moment_label)) %>% 
     #arrondi moment
     mutate(moment_label=ifelse((str_length(moment_label)!=5 & moment_label!=""),
                                 paste0(str_sub(moment_label,1,17),"00:00"),moment_label)) %>% 
     mutate(moment_label=ifelse((str_length(moment_label)==5 & moment_label!=""),
                                paste0(str_sub(moment_label,1,2),":00"),moment_label)) %>%
     relocate(moment_label,.after="moment")
     

# lancement de la fonction de relabelling des x
char <- fct_modif_label_x(Req_Fusion_graph)


graph_HistoWEB4G <- ggplot(Req_Fusion_graph, aes(x=moment,group=1)) +
     geom_line(aes(y=mm_1h_crt), size=1.2, color="Red", linetype="solid") + 
     geom_line(aes(y=mm_1h_ref), size=1, color="Orange", linetype="solid") +

     scale_x_discrete(labels = char) +
     scale_y_continuous(breaks=seq(0,100000,by=5)) +
     
     theme_classic() +
     theme(axis.title.x = element_text(color = "black", size=10, face="italic"),
           axis.text.x = element_text(color="black", size=8, vjust=1, hjust=1, angle=88, face="italic"),
      
           axis.title.y = element_text(color = "black", size=10, face="italic"),
           axis.text.y = element_text(color="black", size=10),
           
           panel.border = element_blank(),
           plot.title = element_text(size=15, hjust=0.5),
           plot.subtitle = element_text(size=10, hjust=0.5, face="italic")) +
     labs(title="Nombre de connexions au site Insee.fr",
          subtitle="comparaison hebdomadaire entre sem. r?f?rence (J-7 ? J-1, trait rouge) et sem. pr?c?dente (J-14 ? J-8, trait orange)",
          x="JourJ-7 vs JourJ - heure", y="effectif",
          caption="Source : Centreon - Gepex")


# sauvegarde des 4 graphiques sur des fichiers .png
#setwd(dir = "C:/Users/siar_ycg8l6/ProgrammesR_Gepex/HistoWEB4G_Connectes")
#png(file="graph_HistoWEB4G.png",width = 1200, height = 450)
#graph_HistoWEB4G
#dev.off()

# encodage des 4 images
#library(base64enc)
#graph_HistoWEB4G_encode <- base64encode("graph_HistoWEB4G.png")
#write.csv2(graph_HistoWEB4G_encode,"graph_HistoWEB4G_encode.csv", row.names = FALSE)
     


########################################################
########################################################
# sauvegarde des resultats dans un repertoire Windows

# Creation dun repertoire avec la date du jour
folder <- paste0("C:/Users/siar_ycg8l6/ProgR/HistoWEB4G_Connectes/Fichiers_date_",format(Sys.time(),'%Y%m%d'))
if (file.exists(folder)) {
     cat("The folder already exists")
} else {
     dir.create(folder, showWarnings = TRUE, recursive = FALSE, mode = "0777")
}

write.csv2(Req_Fusion, 
           paste0(folder,"/HistoWEB4G_Connectes_",str_sub(lib_date_jr,3,10),".csv"),
           row.names=FALSE)


# sauvegarde des 4 graphiques sur des fichiers .png

setwd(dir = folder)
png(file="graph_HistoWEB4G.png",width = 1200, height = 450)
graph_HistoWEB4G
dev.off()

# encodage des 4 images
library(base64enc)
graph_HistoWEB4G_encode <- base64encode("graph_HistoWEB4G.png")
write.csv2(graph_HistoWEB4G_encode,"graph_HistoWEB4G_encode.csv", row.names = FALSE)





                 
                        