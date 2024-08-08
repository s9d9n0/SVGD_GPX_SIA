
# Chargement des packages utiles pour le programme
library(lubridate)
library(dplyr)
library(tidyr)
library(stringr)

# Visualisation NAS Aplishare sur les 15 derniers jours...
rep_gen <- "C:/Users/siar_ycg8l6/ProgR/Analyse_Nas/"


vect_jour <- as.character(Sys.Date())
vect <- vect_jour %>% as.data.frame() %>% rename(listejour=".") %>% 
     mutate(listejour=paste0(listejour," 00:00:00"))
vect_jour <- c()
vect_jour <- append(vect_jour,vect$listejour[1])

datejr_01 <- as.POSIXct(vect_jour) - ddays(15)
datejr_01_char <- datejr_01 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_02 <- as.POSIXct(vect_jour) - ddays(14)
datejr_02_char <- datejr_02 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_03 <- as.POSIXct(vect_jour) - ddays(13)
datejr_03_char <- datejr_03 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_04 <- as.POSIXct(vect_jour) - ddays(12)
datejr_04_char <- datejr_04 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_05 <- as.POSIXct(vect_jour) - ddays(11)
datejr_05_char <- datejr_05 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_06 <- as.POSIXct(vect_jour) - ddays(10)
datejr_06_char <- datejr_06 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_07 <- as.POSIXct(vect_jour) - ddays(9)
datejr_07_char <- datejr_07 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_08 <- as.POSIXct(vect_jour) - ddays(8)
datejr_08_char <- datejr_08 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_09 <- as.POSIXct(vect_jour) - ddays(7)
datejr_09_char <- datejr_09 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_10 <- as.POSIXct(vect_jour) - ddays(6)
datejr_10_char <- datejr_10 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_11 <- as.POSIXct(vect_jour) - ddays(5)
datejr_11_char <- datejr_11 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_12 <- as.POSIXct(vect_jour) - ddays(4)
datejr_12_char <- datejr_12 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_13 <- as.POSIXct(vect_jour) - ddays(3)
datejr_13_char <- datejr_13 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_14 <- as.POSIXct(vect_jour) - ddays(2)
datejr_14_char <- datejr_14 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)
datejr_15 <- as.POSIXct(vect_jour) - ddays(1)
datejr_15_char <- datejr_15 %>% as.character() %>% str_replace_all(pattern = "-",replacement="") %>% str_sub(start=3,end=8)

rm(datejr_01,datejr_02,datejr_03,datejr_04,datejr_05,
   datejr_06,datejr_07,datejr_08,datejr_09,datejr_10,
   datejr_11,datejr_12,datejr_13,datejr_14,datejr_15)

liste_jour <- c(datejr_01_char,datejr_02_char,datejr_03_char,datejr_04_char,datejr_05_char,
                datejr_06_char,datejr_07_char,datejr_08_char,datejr_09_char,datejr_10_char,
                datejr_11_char,datejr_12_char,datejr_13_char,datejr_14_char,datejr_15_char)

liste_jour_df <- data.frame(periode=liste_jour) %>% 
     mutate(periode=paste0(str_sub(periode,3,4),"/",str_sub(periode,5,6)))

################################################################################################
################################################################################################
################################################################################################

#############
#############
# partie DATA

for (i in 1:15){
     df <- read.csv2(paste0(rep_gen,"Quotas_data/df_quotas_data_",liste_jour[i],".csv")) 
     colref_00h <- colnames(df)[8]
     colref_12h <- colnames(df)[20]
     df <- df %>% select(host_name,dc_zone,quartier,vv,type,indicateur,colref_00h,colref_12h)
     df_00h <- df %>% select(-colref_12h) %>% mutate(date=paste0("_",liste_jour[i],"_00h")) %>%
          relocate(date,.after="indicateur") %>% rename(val=colnames(.)[8])
     df_12h <- df %>% select(-colref_00h) %>% mutate(date=paste0("_",liste_jour[i],"_12h")) %>%
          relocate(date,.after="indicateur") %>% rename(val=colnames(.)[8])
     print(nrow(df_00h)) ; print(nrow(df_12h))
     df <- rbind(df_00h,df_12h)
     rm(df_00h,df_12h)
     if (i==1){df_01 <- df}  ; if (i==2){df_02 <- df}  ; if (i==3){df_03 <- df}
     if (i==4){df_04 <- df}  ; if (i==5){df_05 <- df}  ; if (i==6){df_06 <- df}
     if (i==7){df_07 <- df}  ; if (i==8){df_08 <- df}  ; if (i==9){df_09 <- df}
     if (i==10){df_10 <- df} ; if (i==11){df_11 <- df} ; if (i==12){df_12 <- df}
     if (i==13){df_13 <- df} ; if (i==14){df_14 <- df} ; if (i==15){df_15 <- df}
}
     
df_tot <- rbind(df_01,df_02,df_03,df_04,df_05,
                df_06,df_07,df_08,df_09,df_10,
                df_11,df_12,df_13,df_14,df_15)
rm(df_01,df_02,df_03,df_04,df_05,df_06,df_07,df_08,df_09,df_10,df_11,df_12,df_13,df_14,df_15)
rm(i,df)

df_tot <- df_tot %>% pivot_wider(names_from=date, values_from = val) %>%
     as.data.frame() %>% filter(!is.na(vv))
 
type <- "data"

## partie JSON
source(paste0(rep_gen,"ProgR_ConvertJSON_15jrs.R"))

#############
#############
# partie LOGS

for (i in 1:15){
     df <- read.csv2(paste0(rep_gen,"Quotas_logs/df_quotas_logs_",liste_jour[i],".csv")) 
     colref_00h <- colnames(df)[8]
     colref_12h <- colnames(df)[20]
     df <- df %>% select(host_name,dc_zone,quartier,vv,type,indicateur,colref_00h,colref_12h)
     df_00h <- df %>% select(-colref_12h) %>% mutate(date=paste0("_",liste_jour[i],"_00h")) %>%
          relocate(date,.after="indicateur") %>% rename(val=colnames(.)[8])
     df_12h <- df %>% select(-colref_00h) %>% mutate(date=paste0("_",liste_jour[i],"_12h")) %>%
          relocate(date,.after="indicateur") %>% rename(val=colnames(.)[8])
     print(nrow(df_00h)) ; print(nrow(df_12h))
     df <- rbind(df_00h,df_12h)
     rm(df_00h,df_12h)
     if (i==1){df_01 <- df}  ; if (i==2){df_02 <- df}  ; if (i==3){df_03 <- df}
     if (i==4){df_04 <- df}  ; if (i==5){df_05 <- df}  ; if (i==6){df_06 <- df}
     if (i==7){df_07 <- df}  ; if (i==8){df_08 <- df}  ; if (i==9){df_09 <- df}
     if (i==10){df_10 <- df} ; if (i==11){df_11 <- df} ; if (i==12){df_12 <- df}
     if (i==13){df_13 <- df} ; if (i==14){df_14 <- df} ; if (i==15){df_15 <- df}
}

df_tot <- rbind(df_01,df_02,df_03,df_04,df_05,
                df_06,df_07,df_08,df_09,df_10,
                df_11,df_12,df_13,df_14,df_15)
rm(df_01,df_02,df_03,df_04,df_05,df_06,df_07,df_08,df_09,df_10,df_11,df_12,df_13,df_14,df_15)
rm(i,df)


df_tot <- df_tot %>% pivot_wider(names_from=date, values_from = val) %>%
     as.data.frame() %>% filter(!is.na(vv))

type <- "logs"

## partie JSON
source(paste0(rep_gen,"ProgR_ConvertJSON_15jrs.R"))


#############
#############
# partie Periode de suivi

library(jsonlite)

ASv3_json_15jrs <- toJSON(liste_jour_df)
ASv3_json_15jrs <- prettify(ASv3_json_15jrs)


# suppression de lancien fichier de la veille present dans rep_gen
file.remove(paste0(rep_gen,"ASv3_jour_15jrs.json"))
# enregistrement du JSON
write_json(ASv3_json_15jrs,paste0(rep_gen,"ASv3_jour_15jrs.json"))

# copie vers le repertoire de transfert sas ver Zone 50...
rep_sas <- "C:/Users/siar_ycg8l6/ProgR/X_Passerelle_vers_Z050/"

file.copy(paste0(rep_gen,"ASv3_jour_15jrs.json"),
          paste0(rep_sas,"ASv3_jour_15jrs.json"))




