library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)


rm(list = ls());gc();
icustays <- fread("/home/luojiawei/mimiciv/mimic-iv-2.2/icu/icustays.csv.gz")
icustays[1:2,]

# 去掉有多次 stay_id 或多次 hadm_id 的 subject_id
duplicate_stay_ids <- icustays[, .N, by = stay_id][N > 1, stay_id]
duplicate_hadm_ids <- icustays[, .N, by = hadm_id][N > 1, hadm_id]

icustays <- icustays[!subject_id %in% unique(icustays[stay_id %in% duplicate_stay_ids | hadm_id %in% duplicate_hadm_ids, subject_id])]
length(unique(icustays$subject_id))
names(icustays)[8] <- "icu_los"

admissions <- fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/admissions.csv.gz")
admissions[1:2,]
names(admissions)
admissions[, los := as.numeric(difftime(dischtime, admittime, units = "days"))]

ds <- merge(icustays, admissions, by=c("subject_id","hadm_id"), all.x=T)

names(ds)

diag <- fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/diagnoses_icd.csv.gz")
diag[1:2,]
diag <- diag[grep("^(155|C22|C78|1977)", diag$icd_code), ]
length(unique(diag$subject_id))


# 为每个住院计算 Charlson 指数
diag_all <- fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/diagnoses_icd.csv.gz")
diag_all <- diag_all[diag_all$hadm_id %in% unique(diag$hadm_id)]
dim(diag_all)

# 创建一个函数来提取ICD编码的前几个字符
substr_icd <- function(x, n) {
  ifelse(is.na(x), NA, substr(x, 1, n))
}

# 按hadm_id分组计算各种疾病状态
charlson <- diag_all[, {
  # 分别获取ICD-9和ICD-10编码
  icd9 <- ifelse(icd_version == 9, icd_code, NA)
  icd10 <- ifelse(icd_version == 10, icd_code, NA)
  
  # 计算各种疾病的存在情况
  list(
    # 心肌梗塞
    myocardial_infarct = as.numeric(any(
      substr_icd(icd9, 3) %in% c('410', '412') |
      substr_icd(icd10, 3) %in% c('I21', 'I22') |
      substr_icd(icd10, 4) == 'I252'
    )),
    
    # 充血性心力衰竭
    congestive_heart_failure = as.numeric(any(
      substr_icd(icd9, 3) == '428' |
      substr_icd(icd9, 5) %in% c('39891', '40201', '40211', '40291', '40401', '40403',
                                '40411', '40413', '40491', '40493') |
      substr_icd(icd10, 3) %in% c('I43', 'I50') |
      substr_icd(icd10, 4) %in% c('I099', 'I110', 'I130', 'I132', 'I255', 'I420',
                                 'I425', 'I426', 'I427', 'I428', 'I429', 'P290')
    )),
    
    # 外周血管疾病
    peripheral_vascular_disease = as.numeric(any(
      substr_icd(icd9, 3) %in% c('440', '441') |
      substr_icd(icd10, 3) %in% c('I70', 'I71')
    )),
    
    # 脑血管疾病
    cerebrovascular_disease = as.numeric(any(
      (substr_icd(icd9, 3) >= '430' & substr_icd(icd9, 3) <= '438') |
      substr_icd(icd10, 3) %in% c('G45', 'G46') |
      (substr_icd(icd10, 3) >= 'I60' & substr_icd(icd10, 3) <= 'I69')
    )),
    
    # 痴呆
    dementia = as.numeric(any(
      substr_icd(icd9, 3) == '290' |
      substr_icd(icd10, 3) %in% c('F00', 'F01', 'F02', 'F03', 'G30')
    )),
    
    # 慢性肺病
    chronic_pulmonary_disease = as.numeric(any(
      (substr_icd(icd9, 3) >= '490' & substr_icd(icd9, 3) <= '505') |
      substr_icd(icd10, 3) >= 'J40' & substr_icd(icd10, 3) <= 'J47'
    )),
    
    # 肝病(轻度)
    mild_liver_disease = as.numeric(any(
      substr_icd(icd9, 3) %in% c('570', '571') |
      substr_icd(icd10, 3) %in% c('B18', 'K73', 'K74')
    )),
    
    # 肝病(重度)
    severe_liver_disease = as.numeric(any(
      substr_icd(icd9, 4) %in% c('4560', '4561', '4562') |
      substr_icd(icd10, 4) %in% c('K704', 'K711', 'K721', 'K729', 'K765', 'K766', 'K767')
    )),
    
    # 转移性肿瘤
    metastatic_solid_tumor = as.numeric(any(
      substr_icd(icd9, 3) %in% c('196', '197', '198', '199') |
      substr_icd(icd10, 3) %in% c('C77', 'C78', 'C79', 'C80')
    ))
  )
}, by = hadm_id]

# 计算Charlson指数
charlson[, c("myocardial_infarct", "congestive_heart_failure", 
             "peripheral_vascular_disease", "cerebrovascular_disease",
             "dementia", "chronic_pulmonary_disease",
             "mild_liver_disease", "severe_liver_disease",
             "metastatic_solid_tumor") := 
        lapply(.SD, function(x) ifelse(is.na(x), 0, x)), 
        .SDcols = c("myocardial_infarct", "congestive_heart_failure",
                    "peripheral_vascular_disease", "cerebrovascular_disease", 
                    "dementia", "chronic_pulmonary_disease",
                    "mild_liver_disease", "severe_liver_disease",
                    "metastatic_solid_tumor")]

charlson[, charlson_comorbidity_index := 
  myocardial_infarct + congestive_heart_failure +
  peripheral_vascular_disease + cerebrovascular_disease +
  dementia + chronic_pulmonary_disease +
  pmax(mild_liver_disease, 3 * severe_liver_disease) +
  3 * metastatic_solid_tumor
]

charlson[1:3,]
dim(charlson)

# 合并到主数据集
ds <- merge(ds, charlson, by = "hadm_id", all.x = TRUE)

duplicate_subjects <- ds[, .N, by = subject_id][N > 1]
print(paste("重复的subject_id数量:", nrow(duplicate_subjects)))

# 如果存在重复的subject_id，则只保留每个subject_id的第一条记录
if(nrow(duplicate_subjects) > 0) {
  # 按subject_id和入院时间排序
  setorder(ds, subject_id, admittime)
  
  # 只保留每个subject_id的第一条记录
  ds <- ds[, .SD[1], by = subject_id]
  
  print(paste("去重后的记录数:", nrow(ds)))
  print(paste("去重后的唯一subject_id数:", length(unique(ds$subject_id))))
}

ds <- ds[ds$hadm_id %in% unique(diag$hadm_id), ]
dim(ds)
length(unique(ds$subject_id))

patients <- fread("/home/luojiawei/mimiciv/mimic-iv-2.2/hosp/patients.csv.gz")

dim(patients)

ds <- merge(ds, patients, by = "subject_id", all.x = TRUE)
ds <- ds[anchor_age >= 18]
length(unique(ds$subject_id))

unique(ds$anchor_year_group)
ds <- ds[ds$anchor_year_group %in% c("2014 - 2016","2017 - 2019"),]
length(unique(ds$subject_id))
dim(ds)
table(ds$hospital_expire_flag)

# 提取患者数据


drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "mimiciv", 
                 host = "127.0.0.1", port = 5432, 
                 user = "ljw", 
                 password = "123456")
schema_name <- "mimiciv_derived"

table_name <- "first_day_lab"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_lab <- dbGetQuery(con, query)

first_day_lab[1:2,]

first_day_lab <- first_day_lab[first_day_lab$stay_id %in% unique(ds$stay_id),]
dim(first_day_lab)
names(first_day_lab)
names(ds)
ds <- merge(ds, first_day_lab[,-which(names(first_day_lab) %in% c("subject_id","glucose_min","glucose_max")),drop=F], by = c("stay_id"), all.x = TRUE)
dim(ds)

table_name <- "first_day_sofa"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_sofa <- dbGetQuery(con, query)
first_day_sofa[1:2,]
first_day_sofa <- first_day_sofa[first_day_sofa$stay_id %in% unique(ds$stay_id),]
dim(first_day_sofa)
ds <- merge(ds, first_day_sofa[c("stay_id", "sofa")], by = c("stay_id"), all.x = TRUE)
dim(ds)


table_name <- "ventilation"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
ventilation <- dbGetQuery(con, query)

ventilation[1:2,]

ds <- merge(ds, ventilation, by = c("stay_id"), all.x = TRUE)
dim(ds)

ds[1:2,]

# 判断 ICU 入院后 24 小时内是否有机械通气
ds[, vent_within_24h := as.numeric(
  !is.na(starttime) & 
  !is.na(endtime) & 
  difftime(starttime, intime, units = "hours") <= 24 & 
  difftime(starttime, intime, units = "hours") >= 0
)]

# 如果有重复的 stay_id，处理 vent_within_24h
vent_status <- ds[, .(vent_within_24h = as.numeric(any(vent_within_24h == 1))), 
                 by = stay_id]

# 更新之前的代码，加入 vent_within_24h 到需要排除的列
cols_to_exclude <- c("starttime", "endtime", "ventilation_status", "vent_within_24h")
ds_last <- ds[, .SD[.N], by = stay_id, .SDcols = !cols_to_exclude]

ds <- merge(ds_last, vent_status, by = "stay_id")
ds[1:2,]
dim(ds)
table(ds$vent_within_24h)


table_name <- "oasis"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
oasis <- dbGetQuery(con, query)

oasis[1:2,]

ds <- merge(ds, oasis[c("stay_id", "oasis")], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]


table_name <- "sapsii"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
sapsii <- dbGetQuery(con, query)

setDT(sapsii)
sapsii_first <- sapsii[order(starttime), .SD[1], by = stay_id][, .(stay_id, sapsii)]
sapsii_first[1:2,]

ds <- merge(ds, sapsii_first, by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]



table_name <- "sirs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
sirs <- dbGetQuery(con, query)
sirs[1:2,]

ds <- merge(ds, sirs[c("stay_id", "sirs")], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "lods"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
lods <- dbGetQuery(con, query)
lods[1:2,]

ds <- merge(ds, lods[c("stay_id", "lods")], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]


table_name <- "apsiii"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
apsiii <- dbGetQuery(con, query)
apsiii[1:2,]

ds <- merge(ds, apsiii[c("stay_id", "apsiii")], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]


table_name <- "meld"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
meld <- dbGetQuery(con, query)
meld[1:2,]

ds <- merge(ds, meld[c("stay_id", "meld")], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "first_day_vitalsign"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_vitalsign <- dbGetQuery(con, query)
first_day_vitalsign[1:2,]

ds <- merge(ds, first_day_vitalsign[,-which(names(first_day_vitalsign) %in% c("subject_id")),drop=F], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "first_day_gcs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_gcs <- dbGetQuery(con, query)
first_day_gcs[1:2,]

ds <- merge(ds, first_day_gcs[,-which(names(first_day_gcs) %in% c("subject_id")),drop=F], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "first_day_bg"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_bg <- dbGetQuery(con, query)
first_day_bg[1:2,]

# paste0(names(first_day_bg), collapse = "','")

ds <- merge(ds, first_day_bg[,c('stay_id','lactate_min','lactate_max','ph_min','ph_max','so2_min','so2_max','po2_min','po2_max','pco2_min','pco2_max','aado2_min','aado2_max','baseexcess_min','baseexcess_max','totalco2_min','totalco2_max','carboxyhemoglobin_min','carboxyhemoglobin_max','methemoglobin_min','methemoglobin_max'),drop=F], by = c("stay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table(ds$hospital_expire_flag)
names(ds)


# 关闭连接
dbDisconnect(con)

fwrite(ds, file = "mimic4_肝癌患者数据.csv", row.names = FALSE)



















