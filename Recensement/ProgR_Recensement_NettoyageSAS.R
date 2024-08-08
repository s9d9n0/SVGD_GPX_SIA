
## Chargement des differents packages utilises dans le programme ----
library(dplyr)
library(stringr)
library(tidyr)
library(readr)
library(RMariaDB)
library(lubridate)

# rep_sas <- "E:/X_Passerelle_vers_Z050/"
rep_sas <- "C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/"

###

# liste des fichiers csv presents dans le repertoire sas et 
# enregistrement dans un dataframe listing
listing <- list.files(rep_sas) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich) %>% 
     filter(str_detect(liste_fich,"Recensement")) 

# suppression de tous les fichiers dans le repertoire sas
for (i in 1:nrow(listing)){
     cat(paste0("numero ",i," ",listing$liste_fich[i],"\n"))
     file.remove(paste0(rep_sas,listing$liste_fich[i]))
}


##################################################################################################
##################################################################################################

# nettoyage egalement du cote du fichier dorigine (on ne garde que le fichier .zip)...

date_jour <- Sys.time()

part_date <- as.character(Sys.Date())
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))
part_seconde <- as.character(round(second(date_jour),0))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
if (str_length(part_seconde)==1){part_seconde <- paste0("0",part_seconde)}
lib_date_jr <- paste0(part_date,"_",part_heure,"h",part_minute,"m",part_seconde,"s")

rm(part_date,part_heure,part_minute,part_seconde)

partie_datejour <- paste0(str_sub(lib_date_jr,1,4),
                          str_sub(lib_date_jr,6,7),
                          str_sub(lib_date_jr,9,10))

rep_fichier <- paste0("C:/Users/siar_ycg8l6/ProgR/Centreon_Recensement/",
                      "Fichiers_date_",
                      partie_datejour)


# liste des fichiers csv presents dans le repertoire sas et 
# enregistrement dans un dataframe listing
listing <- list.files(rep_fichier) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich) %>% 
     filter(str_detect(liste_fich,".csv")) 

# suppression de tous les fichiers dans le repertoire sas
for (i in 1:nrow(listing)){
     cat(paste0("numero ",i," ",listing$liste_fich[i],"\n"))
     file.remove(paste0(rep_fichier,"/",listing$liste_fich[i]))
}




