
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
     filter(str_detect(liste_fich,"df_Dns") | str_detect(liste_fich,"df_PaloAlto")) 

# suppression de tous les fichiers dans le repertoire sas
for (i in 1:nrow(listing)){
     cat(paste0("numero ",i," ",listing$liste_fich[i],"\n"))
     file.remove(paste0(rep_sas,listing$liste_fich[i]))
}





