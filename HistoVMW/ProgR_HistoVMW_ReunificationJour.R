
library(RMariaDB)
library(ggplot2)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)

date_jour <- Sys.time()
part_heure <- as.character(hour(date_jour))
part_minute <- as.character(minute(date_jour))

if (str_length(part_heure)==1){part_heure <- paste0("0",part_heure)}
if (str_length(part_minute)==1){part_minute <- paste0("0",part_minute)}
lib_time <- paste0("_",part_heure,"h",part_minute)

rm(part_heure,part_minute)

part_date  <- as.character(Sys.Date()-ddays(1))
part_date2 <- str_remove_all(part_date,"-")

# pour tester...
# part_date <- "2023-07-17"
# part_date2 <- "20230717"


rep_fich <- paste0("C:/Users/siar_ycg8l6/ProgR/HistoVMW_EsxListeHost/Fichiers_date_",part_date2,"/")
setwd(rep_fich)

rep_sas <- paste0("C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/")


##########
##########
# liste des fichiers csv presents dans le repertoire sas et 
# enregistrement dans un dataframe listing
listing <- list.files(rep_fich) %>% as.data.frame() %>% 
     rename(liste_fich=".") %>% arrange(liste_fich)

listing_EsxiVM <- listing %>%
     filter(str_detect(liste_fich,"List_EsxiVM") & str_detect(liste_fich,"csv")) %>%
     arrange(liste_fich)


if (nrow(listing_EsxiVM)%%2==0){
     part0 <- 0
     part1 <- (nrow(listing_EsxiVM))/6*1
     part2 <- (nrow(listing_EsxiVM))/6*2
     part3 <- (nrow(listing_EsxiVM))/6*3
     part4 <- (nrow(listing_EsxiVM))/6*4
     part5 <- (nrow(listing_EsxiVM))/6*5
     part6 <- (nrow(listing_EsxiVM))/6*6
} else {
     part0 <- 0
     part1 <- round((nrow(listing_EsxiVM))/6*1,0)
     part2 <- round((nrow(listing_EsxiVM))/6*2,0)
     part3 <- round((nrow(listing_EsxiVM))/6*3,0)
     part4 <- round((nrow(listing_EsxiVM))/6*4,0)
     part5 <- round((nrow(listing_EsxiVM))/6*5,0)
     part6 <- round((nrow(listing_EsxiVM))/6*6,0)
}

vect_part <- c(part0,part1,part2,part3,part4,part5,part6)


##########
##########
# PARTIE 1
# copie de tous les fichiers datareq dans le repertoire de reference
for (num in 1:6){
     for (i in (vect_part[num]+1):vect_part[num+1]){
          cat(paste0("numero ",i," ",listing_EsxiVM$liste_fich[i],"\n"))
          # concatenation des fichiers de type EsxiVM
          if (i==(part0+1) | i==(part1+1) | i==(part2+1) |
              i==(part3+1) | i==(part4+1) | i==(part5+1)) {
               file.copy(paste0(rep_fich,listing_EsxiVM$liste_fich[i]),
                         paste0(rep_fich,"df_EsxiVM_part",num,"_",part_date,lib_time,".csv"))
          } else {
               df_EsxiVM <- read.csv2(paste0(rep_fich,"df_EsxiVM_part",num,"_",part_date,lib_time,".csv"))
               df_copy <- read.csv2(paste0(rep_fich,listing_EsxiVM$liste_fich[i]))
               df_EsxiVM <- rbind(df_EsxiVM,df_copy)
               write.csv2(df_EsxiVM,paste0(rep_fich,"df_EsxiVM_part",num,"_",part_date,lib_time,".csv"),row.names=FALSE)
          }
          # insertion du fichier unitaire dans un .zip puis suppression
          zip(paste0("df_EsxiVM_part",num,"_",part_date,lib_time,".zip"),
          paste0(listing_EsxiVM$liste_fich[i]))
          file.remove(paste0(rep_fich,listing_EsxiVM$liste_fich[i]))
     }
     rm(i,df_copy) 
}


zip(paste0("df_EsxiVM_",part_date,lib_time,".zip"),
    paste0("df_EsxiVM_part1_",part_date,lib_time,".csv"))
zip(paste0("df_EsxiVM_",part_date,lib_time,".zip"),
    paste0("df_EsxiVM_part2_",part_date,lib_time,".csv"))
zip(paste0("df_EsxiVM_",part_date,lib_time,".zip"),
    paste0("df_EsxiVM_part3_",part_date,lib_time,".csv"))
zip(paste0("df_EsxiVM_",part_date,lib_time,".zip"),
    paste0("df_EsxiVM_part4_",part_date,lib_time,".csv"))
zip(paste0("df_EsxiVM_",part_date,lib_time,".zip"),
    paste0("df_EsxiVM_part5_",part_date,lib_time,".csv"))
zip(paste0("df_EsxiVM_",part_date,lib_time,".zip"),
    paste0("df_EsxiVM_part6_",part_date,lib_time,".csv"))

# Transfert du fichier zip global dans le repertoire sas vers la z050
file.copy(paste0(rep_fich,"df_EsxiVM_",part_date,lib_time,".zip"),
          paste0(rep_sas,"df_EsxiVM_",part_date,lib_time,".zip"))


# Transfert des fichiers dans le repertoire sas vers la z050
# df_EsxiVM <- read.csv2(paste0(rep_fich,"df_EsxiVM_",part_date,lib_time,".csv"))
# 
# write.csv2(df_EsxiVM, 
#            paste0(rep_sas,"df_EsxiVM_",part_date,lib_time,".csv"),
#            row.names=FALSE)




