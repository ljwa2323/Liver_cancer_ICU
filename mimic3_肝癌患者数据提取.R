library(data.table)
library(dplyr)
library(magrittr)
library(mice)
library(parallel)
library(lubridate)
library(RPostgreSQL)
library(stringr)

rm(list = ls());gc();

icustays <- fread("/home/luojiawei/mimiciii/mimic3_data/ICUSTAYS.csv")
names(icustays) <- tolower(names(icustays))
icustays[1:2,]

# 去掉有多次 stay_id 或多次 hadm_id 的 subject_id
duplicate_icustay_ids <- icustays[, .N, by = icustay_id][N > 1, icustay_id]
duplicate_hadm_ids <- icustays[, .N, by = hadm_id][N > 1, hadm_id]

icustays <- icustays[!subject_id %in% unique(icustays[icustay_id %in% duplicate_icustay_ids | hadm_id %in% duplicate_hadm_ids, subject_id])]
length(unique(icustays$subject_id))
names(icustays)[12] <- "icu_los"

admissions <- fread("/home/luojiawei/mimiciii/mimic3_data/ADMISSIONS.csv")
names(admissions) <- tolower(names(admissions))
admissions[1:2,]
names(admissions)
admissions[, los := as.numeric(difftime(dischtime, admittime, units = "days"))]

ds <- merge(icustays, admissions[,-which(names(admissions) %in% c("row_id")),with=F], by=c("subject_id","hadm_id"), all.x=T)
ds[1:2,]
length(unique(ds$subject_id))

diag <- fread("/home/luojiawei/mimiciii/mimic3_data/DIAGNOSES_ICD.csv")
names(diag) <- tolower(names(diag))
diag[1:2,]
diag <- diag[grep("^(155|C22|C78|1977)", diag$icd9_code), ]
length(unique(diag$subject_id))


# 为每个住院计算 Charlson 指数
diag_all <- fread("/home/luojiawei/mimiciii/mimic3_data/DIAGNOSES_ICD.csv")
names(diag_all) <- tolower(names(diag_all))
diag_all <- diag_all[diag_all$hadm_id %in% unique(diag$hadm_id)]
dim(diag_all)
diag_all[1:3,]

# 创建一个函数来提取ICD编码的前几个字符
substr_icd <- function(x, n) {
  ifelse(is.na(x), NA, substr(x, 1, n))
}

# 按hadm_id分组计算各种疾病状态
charlson <- diag_all[, {
  # 所有编码都是ICD-9
  icd9 <- icd9_code
  
  # 计算各种疾病的存在情况
  list(
    # 心肌梗塞
    myocardial_infarct = as.numeric(any(
      substr_icd(icd9, 3) %in% c('410', '412')
    )),
    
    # 充血性心力衰竭
    congestive_heart_failure = as.numeric(any(
      substr_icd(icd9, 3) == '428' |
      substr_icd(icd9, 5) %in% c('39891', '40201', '40211', '40291', '40401', '40403',
                                '40411', '40413', '40491', '40493')
    )),
    
    # 外周血管疾病
    peripheral_vascular_disease = as.numeric(any(
      substr_icd(icd9, 3) %in% c('440', '441')
    )),
    
    # 脑血管疾病
    cerebrovascular_disease = as.numeric(any(
      substr_icd(icd9, 3) >= '430' & substr_icd(icd9, 3) <= '438'
    )),
    
    # 痴呆
    dementia = as.numeric(any(
      substr_icd(icd9, 3) == '290'
    )),
    
    # 慢性肺病
    chronic_pulmonary_disease = as.numeric(any(
      substr_icd(icd9, 3) >= '490' & substr_icd(icd9, 3) <= '505'
    )),
    
    # 肝病(轻度)
    mild_liver_disease = as.numeric(any(
      substr_icd(icd9, 3) %in% c('570', '571')
    )),
    
    # 肝病(重度)
    severe_liver_disease = as.numeric(any(
      substr_icd(icd9, 4) %in% c('4560', '4561', '4562')
    )),
    
    # 转移性肿瘤
    metastatic_solid_tumor = as.numeric(any(
      substr_icd(icd9, 3) %in% c('196', '197', '198', '199')
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
length(unique(ds$subject_id))

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

table(ds$hospital_expire_flag)

names(ds)



patients <- fread("/home/luojiawei/mimiciii/mimic3_data/PATIENTS.csv")
names(patients) <- tolower(names(patients))
patients[1:2,]
dim(patients)

ds <- merge(ds, patients[,-which(names(patients) %in% c("row_id")),with=F], by = "subject_id", all.x = TRUE)
ds[, anchor_age := as.integer(difftime(admittime, dob, units = "weeks") / 52.25)]
ds[1:2,]
ds <- ds[anchor_age >= 18]
dim(ds)
table(ds$hospital_expire_flag)



# 提取患者数据

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "mimiciii", 
                 host = "127.0.0.1", port = 5432, 
                 user = "ljw", 
                 password = "123456")
schema_name <- "public"

table_name <- "labs_first_day"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_lab <- dbGetQuery(con, query)

first_day_lab[1:2,]

first_day_lab <- first_day_lab[first_day_lab$icustay_id %in% unique(ds$icustay_id),]
dim(first_day_lab)
names(first_day_lab)
names(ds)
ds <- merge(ds, first_day_lab[,-which(names(first_day_lab) %in% c("subject_id","glucose_min","glucose_max","hadm_id")),drop=F], by = c("icustay_id"), all.x = TRUE)
dim(ds)

table_name <- "sofa"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
sofa <- dbGetQuery(con, query)
sofa[1:2,]
sofa <- sofa[sofa$icustay_id %in% unique(ds$icustay_id),]
dim(sofa)
ds <- merge(ds, sofa[c("icustay_id", "sofa")], by = c("icustay_id"), all.x = TRUE)
dim(ds)



table_name <- "ventilation_first_day"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
ventilation <- dbGetQuery(con, query)

ventilation[1:2,]

ds <- merge(ds, ventilation[,-which(names(ventilation) %in% c("subject_id","hadm_id")),drop=F], by = c("icustay_id"), all.x = TRUE)
dim(ds)

ds[1:2,]

table_name <- "oasis"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
oasis <- dbGetQuery(con, query)

oasis[1:2,]

ds <- merge(ds, oasis[c("icustay_id", "oasis")], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]


table_name <- "sapsii"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
sapsii <- dbGetQuery(con, query)

sapsii[1:2,]

ds <- merge(ds, sapsii[c("icustay_id", "sapsii")], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "sirs"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
sirs <- dbGetQuery(con, query)
sirs[1:2,]

ds <- merge(ds, sirs[c("icustay_id", "sirs")], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "lods"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
lods <- dbGetQuery(con, query)
lods[1:2,]

ds <- merge(ds, lods[c("icustay_id", "lods")], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "apsiii"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
apsiii <- dbGetQuery(con, query)
apsiii[1:2,]

ds <- merge(ds, apsiii[c("icustay_id", "apsiii")], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "meld"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
meld <- dbGetQuery(con, query)
meld[1:2,]

ds <- merge(ds, meld[c("icustay_id", "meld")], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]

table_name <- "vitals_first_day"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_vitalsign <- dbGetQuery(con, query)
first_day_vitalsign[1:2,]

ds <- merge(ds, first_day_vitalsign[,-which(names(first_day_vitalsign) %in% c("subject_id","hadm_id")),drop=F], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]


table_name <- "gcs_first_day"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
first_day_gcs <- dbGetQuery(con, query)
first_day_gcs[1:2,]

ds <- merge(ds, first_day_gcs[,-which(names(first_day_gcs) %in% c("subject_id","hadm_id")),drop=F], by = c("icustay_id"), all.x = TRUE)
dim(ds)
ds[1:2,]


table_name <- "blood_gas_first_day"
query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
blood_gas <- dbGetQuery(con, query)
blood_gas[1:2,]

# paste0(names(blood_gas), collapse = "','")

# 自定义安全的最小值和最大值函数
safe_min <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  result <- min(x, na.rm = TRUE)
  if (is.infinite(result)) return(NA_real_)
  return(result)
}

safe_max <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  result <- max(x, na.rm = TRUE)
  if (is.infinite(result)) return(NA_real_)
  return(result)
}

# 确保blood_gas是data.table
setDT(blood_gas)

# 直接计算blood_gas中每个stay_id的最大最小值
last_measurements <- blood_gas[, .(
  ph_min = safe_min(ph),
  ph_max = safe_max(ph),
  so2_min = safe_min(so2),
  so2_max = safe_max(so2),
  po2_min = safe_min(po2),
  po2_max = safe_max(po2),
  pco2_min = safe_min(pco2),
  pco2_max = safe_max(pco2),
  aado2_min = safe_min(aado2),
  aado2_max = safe_max(aado2),
  baseexcess_min = safe_min(baseexcess),
  baseexcess_max = safe_max(baseexcess),
  totalco2_min = safe_min(totalco2),
  totalco2_max = safe_max(totalco2),
  carboxyhemoglobin_min = safe_min(carboxyhemoglobin),
  carboxyhemoglobin_max = safe_max(carboxyhemoglobin),
  methemoglobin_min = safe_min(methemoglobin),
  methemoglobin_max = safe_max(methemoglobin)
), by = icustay_id]


last_measurements[1:2,]

# 将结果与主数据集合并
ds <- merge(ds, last_measurements, by = "icustay_id", all.x = TRUE)

dim(ds)
ds[1:2,]

names(ds)

# 关闭连接
dbDisconnect(con)

fwrite(ds, file = "mimic3_肝癌患者数据.csv", row.names = FALSE)

















