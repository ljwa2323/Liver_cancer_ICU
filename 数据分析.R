rm(list = ls());gc();

# 加载必要的包
library(data.table)
library(tableone)
library(tibble)
# 读取 MIMIC-III 数据集
ds_mimic3 <- fread("/home/luojiawei/zhengzhuo_paper3/mimic3_肝癌患者数据.csv")

# 读取 MIMIC-IV 数据集
ds_mimic4 <- fread("/home/luojiawei/zhengzhuo_paper3/mimic4_肝癌患者数据.csv")

# 查看数据集基本信息
print("MIMIC-III 数据集维度：")
print(dim(ds_mimic3))
print("MIMIC-IV 数据集维度：")
print(dim(ds_mimic4))

# 显示数据集的前几行
head(ds_mimic3)
head(ds_mimic4)

names(ds_mimic3)
names(ds_mimic4)

# 统一列名
setnames(ds_mimic3, 
         old = c("icustay_id", "row_id", "ethnicity", "platelet_min", "platelet_max",
                "sysbp_min", "sysbp_max", "sysbp_mean",
                "diasbp_min", "diasbp_max", "diasbp_mean",
                "meanbp_min", "meanbp_max", "meanbp_mean",
                "heartrate_min", "heartrate_max", "heartrate_mean",
                "resprate_min", "resprate_max", "resprate_mean",
                "tempc_min", "tempc_max", "tempc_mean",
                "mingcs", "bilirubin_min", "bilirubin_max"),
         new = c("stay_id", "row_id", "race", "platelets_min", "platelets_max",
                "sbp_min", "sbp_max", "sbp_mean",
                "dbp_min", "dbp_max", "dbp_mean",
                "mbp_min", "mbp_max", "mbp_mean",
                "heart_rate_min", "heart_rate_max", "heart_rate_mean",
                "resp_rate_min", "resp_rate_max", "resp_rate_mean",
                "temperature_min", "temperature_max", "temperature_mean",
                "gcs_min", "bilirubin_total_min", "bilirubin_total_max"))

# 定义共同的列顺序
common_cols <- c(
    # 标识符
    "stay_id", "subject_id", "hadm_id",
    # 住院信息
    "first_careunit", "last_careunit", "intime", "outtime", "icu_los",
    "admittime", "dischtime", "deathtime", "admission_type",
    "admission_location", "discharge_location", "insurance",
    # 人口统计学
    "language", "marital_status", "race", "gender", "anchor_age",
    # 并发症指数
    "charlson_comorbidity_index", "myocardial_infarct", "congestive_heart_failure",
    "peripheral_vascular_disease", "cerebrovascular_disease", "dementia",
    "chronic_pulmonary_disease", "mild_liver_disease", "severe_liver_disease",
    "metastatic_solid_tumor",
    # 评分
    "sofa", "oasis", "sapsii", "sirs", "lods", "apsiii", "meld",
    # 实验室检查
    "albumin_min", "albumin_max",
    "bilirubin_total_min", "bilirubin_total_max",
    "creatinine_min", "creatinine_max",
    "hematocrit_min", "hematocrit_max",
    "hemoglobin_min", "hemoglobin_max",
    "platelets_min", "platelets_max",
    "wbc_min", "wbc_max",
    "inr_min", "inr_max",
    "pt_min", "pt_max",
    "ptt_min", "ptt_max",
    # 生命体征
    "heart_rate_min", "heart_rate_max", "heart_rate_mean",
    "sbp_min", "sbp_max", "sbp_mean",
    "dbp_min", "dbp_max", "dbp_mean",
    "mbp_min", "mbp_max", "mbp_mean",
    "resp_rate_min", "resp_rate_max", "resp_rate_mean",
    "temperature_min", "temperature_max", "temperature_mean",
    "spo2_min", "spo2_max", "spo2_mean",
    # 其他重要指标
    "glucose_min", "glucose_max", "glucose_mean",
    "lactate_min", "lactate_max",
    # 结局
    "hospital_expire_flag"
)

# 选择并重排列
ds_mimic3_selected <- ds_mimic3[, ..common_cols]
ds_mimic4_selected <- ds_mimic4[, ..common_cols]

dim(ds_mimic3_selected)
dim(ds_mimic4_selected)

names(ds_mimic3_selected)
names(ds_mimic4_selected)


# 添加数据来源标识列
ds_mimic3_selected[, dataset := "MIMIC-III"]
ds_mimic4_selected[, dataset := "MIMIC-IV"]

# 合并数据集
ds_combined <- rbindlist(list(ds_mimic3_selected, ds_mimic4_selected), use.names = TRUE)

# 查看合并后的数据集维度
print("合并后的数据集维度：")
print(dim(ds_combined))

# 检查每个数据集的样本量
print("各数据集样本量：")
print(table(ds_combined$dataset))

str(ds_combined)
ds_combined <- as.data.frame(ds_combined)

# 重新编码种族变量
ds_combined$race <- factor(
  case_when(
    grepl("WHITE", ds_combined$race) | grepl("PORTUGUESE", ds_combined$race) ~ "White",
    grepl("BLACK|HAITIAN|AFRICAN", ds_combined$race) ~ "Black",
    grepl("ASIAN|KOREAN|VIETNAMESE|CAMBODIAN", ds_combined$race) ~ "Asian",
    grepl("HISPANIC|LATINO", ds_combined$race) ~ "Hispanic",
    TRUE ~ "Other/Unknown"
  )
)

ds_combined$marital_status <- factor(
  case_when(
    grepl("MARRIED|LIFE PARTNER", ds_combined$marital_status) ~ "Married/Partner",
    grepl("SINGLE", ds_combined$marital_status) ~ "Single",
    grepl("DIVORCED|SEPARATED", ds_combined$marital_status) ~ "Divorced/Separated",
    grepl("WIDOWED", ds_combined$marital_status) ~ "Widowed",
    TRUE ~ "Unknown"
  )
)

# 重新编码保险类型
ds_combined$insurance <- factor(
  case_when(
    grepl("Medicare", ds_combined$insurance) ~ "Medicare",
    grepl("Medicaid", ds_combined$insurance) ~ "Medicaid",
    grepl("Private", ds_combined$insurance) ~ "Private",
    grepl("Government", ds_combined$insurance) ~ "Government",
    grepl("Self Pay", ds_combined$insurance) ~ "Self Pay",
    TRUE ~ "Other"
  )
)


# 所有需要分析的变量
vars <- c(
    # 人口统计学
    "gender", "anchor_age", "race", "marital_status", "insurance", "icu_los",
    # 并发症指数
    "myocardial_infarct", "congestive_heart_failure",
    "peripheral_vascular_disease", "cerebrovascular_disease", "dementia",
    "chronic_pulmonary_disease", "mild_liver_disease", "severe_liver_disease",
    "metastatic_solid_tumor","charlson_comorbidity_index",
    # 评分
    "sofa", "oasis", "sapsii", "sirs", "lods", "apsiii", "meld",
    # 实验室检查
    "albumin_min", "albumin_max",
    "bilirubin_total_min", "bilirubin_total_max",
    "creatinine_min", "creatinine_max",
    "hematocrit_min", "hematocrit_max",
    "hemoglobin_min", "hemoglobin_max",
    "platelets_min", "platelets_max",
    "wbc_min", "wbc_max",
    "inr_min", "inr_max",
    "pt_min", "pt_max",
    "ptt_min", "ptt_max",
    # 其他重要指标
    "glucose_min", "glucose_max", "glucose_mean",
    "lactate_min", "lactate_max",
    # 生命体征
    "heart_rate_min", "heart_rate_max", "heart_rate_mean",
    "sbp_min", "sbp_max", "sbp_mean",
    "dbp_min", "dbp_max", "dbp_mean",
    "mbp_min", "mbp_max", "mbp_mean",
    "resp_rate_min", "resp_rate_max", "resp_rate_mean",
    "temperature_min", "temperature_max", "temperature_mean",
    "spo2_min", "spo2_max", "spo2_mean",
    # 结局
    "hospital_expire_flag"
)

# 分类变量
str_vars <- c(
    "gender", "race",  "marital_status", "insurance",
    "myocardial_infarct", "congestive_heart_failure",
    "peripheral_vascular_disease", "cerebrovascular_disease", "dementia",
    "chronic_pulmonary_disease", "mild_liver_disease", "severe_liver_disease",
    "metastatic_solid_tumor", "hospital_expire_flag"
)

# 连续变量（通过从vars中排除str_vars得到）
num_vars <- vars[!(vars %in% str_vars)]
ds_combined[,str_vars] <- lapply(ds_combined[,str_vars], function(x) {x <- as.character(x); x<-factor(x); x})
ds_combined[,num_vars] <- lapply(ds_combined[,num_vars], as.numeric)
non_norm <- c()

# Grouped summary table
tab <- CreateTableOne(vars = vars, factorVars = str_vars, addOverall = TRUE, data = ds_combined, strata = "dataset")

# Print table
tab1<-print(tab, nonnormal = non_norm, showAllLevels = T)
rn<-row.names(tab1)
tab1<-as.data.frame(tab1)
tab1 <- rownames_to_column(tab1, var = "Variable")
tab1$Variable<-rn

tab1

write.csv(tab1, file="./Table1.csv", row.names=F, fileEncoding='gb2312')


fwrite(ds_combined, file="./ds_combined.csv", row.names=F)


# 加载 mice 包
library(mice)

# 分别获取 MIMIC-III 和 MIMIC-IV 数据
mimic3_data <- ds_combined[ds_combined$dataset == "MIMIC-III", ]
mimic4_data <- ds_combined[ds_combined$dataset == "MIMIC-IV", ]

# 准备用于插补的变量（排除 hospital_expire_flag 和 dataset）
impute_vars <- vars[vars != "hospital_expire_flag"]

# MIMIC-III 数据插补
set.seed(123) # 设置随机种子以确保可重复性
mice_mimic3 <- mice(mimic3_data[, impute_vars], m = 1, maxit = 10, method = "pmm", printFlag = FALSE)
mimic3_complete <- complete(mice_mimic3, 1) # 使用第一个插补数据集
mimic3_complete$dataset <- "MIMIC-III"
mimic3_complete$hospital_expire_flag <- mimic3_data$hospital_expire_flag

# MIMIC-IV 数据插补
set.seed(456)
mice_mimic4 <- mice(mimic4_data[, impute_vars], m = 1, maxit = 10, method = "pmm", printFlag = FALSE)
mimic4_complete <- complete(mice_mimic4, 1)
mimic4_complete$dataset <- "MIMIC-IV"
mimic4_complete$hospital_expire_flag <- mimic4_data$hospital_expire_flag

# 合并插补后的数据集
ds_imputed <- rbind(mimic3_complete, mimic4_complete)

fwrite(ds_imputed[,c(vars, "dataset"),drop=F],file="ds_model.csv", row.names=F)



