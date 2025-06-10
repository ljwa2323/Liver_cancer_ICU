# 读取预测结果文件
predictions <- read.csv("/home/luojiawei/zhengzhuo_paper3/predictions_with_probas.csv")


names(predictions)[3:6] <- c("LR","RF","LGBM","XGB")


# 读取组合数据集文件
ds_combined <- read.csv("/home/luojiawei/zhengzhuo_paper3/ds_combined.csv")

names(ds_combined)
ds_combined <- cbind(ds_combined[,c(84,85,18,19,6,10,11)], predictions[,3:6])

ds_combined[1:2,]

ds_combined <- ds_combined[ds_combined$dataset=="MIMIC-IV",]
summary(ds_combined)



# 加载必要的包
library(survival)
library(survminer)

# 首先将字符串转换为日期时间格式
ds_combined$intime <- as.Date(ds_combined$intime)
ds_combined$dischtime <- as.Date(ds_combined$dischtime)
ds_combined$deathtime <- as.Date(ds_combined$deathtime)

# 计算住院时间（以天为单位）和生存状态
ds_combined$time <- as.numeric(difftime(
  as.Date(ifelse(!is.na(ds_combined$deathtime), 
         ds_combined$deathtime, 
         ds_combined$dischtime),origin = "1970-1-1"),
  ds_combined$intime, 
  units = "days"
))


# 为每个模型创建高危/低危分组
ds_combined$LR_group <- ifelse(ds_combined$LR > 0.31158, "High Risk", "Low Risk")
ds_combined$RF_group <- ifelse(ds_combined$RF > 0.58000, "High Risk", "Low Risk")
ds_combined$LGBM_group <- ifelse(ds_combined$LGBM > 0.96801, "High Risk", "Low Risk")
ds_combined$XGB_group <- ifelse(ds_combined$XGB > 0.94214, "High Risk", "Low Risk")

# 检查time变量的类型并强制转换为数值
ds_combined$time <- as.numeric(ds_combined$time)

# 检查hospital_expire_flag是否为数值
ds_combined$hospital_expire_flag <- as.numeric(as.factor(ds_combined$hospital_expire_flag)) - 1

# 让我们先查看一下数据
print(head(ds_combined$time))
print(head(ds_combined$hospital_expire_flag))




# 加载必要的包
library(survival)
library(survminer)
library(gridExtra)


# 为 LR 模型创建 KM 图
fit_lr <- survfit(Surv(time, hospital_expire_flag) ~ LR_group, data = ds_combined)
lr_diff <- survdiff(Surv(time, hospital_expire_flag) ~ LR_group, data = ds_combined)
lr_chisq <- round(lr_diff$chisq, 2)
lr_plot <- ggsurvplot(fit_lr,
           data = ds_combined,
           pval = F,
           pval.method = F,
           conf.int = TRUE,
           risk.table = TRUE,
           title = "Logistic Regression",
           legend.labs = c("Low Risk", "High Risk"),
           palette = c("blue", "red"),
           risk.table.height = 0.25,
           xlab = "Time (days)",
           ylab = "Survival probability")
lr_plot$plot <- lr_plot$plot + annotate("text", x = max(ds_combined$time)/4, y = 0.2, 
                                       label = paste("Log-rank χ² =", round(lr_diff$chisq, 2), "\nP-value =", format.pval(lr_diff$pvalue, digits = 2)), size = 4)


# 为 RF 模型创建 KM 图
fit_rf <- survfit(Surv(time, hospital_expire_flag) ~ RF_group, data = ds_combined)
rf_diff <- survdiff(Surv(time, hospital_expire_flag) ~ RF_group, data = ds_combined)
rf_chisq <- round(rf_diff$chisq, 2)
rf_plot <- ggsurvplot(fit_rf,
           data = ds_combined,
           pval = F,
           pval.method = F,
           conf.int = TRUE,
           risk.table = TRUE,
           title = "Random Forest",
           legend.labs = c("Low Risk", "High Risk"),
           palette = c("blue", "red"),
           risk.table.height = 0.25,
           xlab = "Time (days)",
           ylab = "Survival probability")
rf_plot$plot <- rf_plot$plot + annotate("text", x = max(ds_combined$time)/4, y = 0.2, 
                                       label = paste("Log-rank χ² =", round(rf_diff$chisq, 2), "\nP-value =", format.pval(rf_diff$pvalue, digits = 2)), size = 4)

# 为 LGBM 模型创建 KM 图
fit_lgbm <- survfit(Surv(time, hospital_expire_flag) ~ LGBM_group, data = ds_combined)
lgbm_diff <- survdiff(Surv(time, hospital_expire_flag) ~ LGBM_group, data = ds_combined)
lgbm_chisq <- round(lgbm_diff$chisq, 2)
lgbm_plot <- ggsurvplot(fit_lgbm,
           data = ds_combined,
           pval = F,
           pval.method = F,
           conf.int = TRUE,
           risk.table = TRUE,
           title = "LightGBM",
           legend.labs = c("Low Risk", "High Risk"),
           palette = c("blue", "red"),
           risk.table.height = 0.25,
           xlab = "Time (days)",
           ylab = "Survival probability")
lgbm_plot$plot <- lgbm_plot$plot + annotate("text", x = max(ds_combined$time)/4, y = 0.2, 
                                          label = paste("Log-rank χ² =", round(lgbm_diff$chisq, 2), "\nP-value =", format.pval(lgbm_diff$pvalue, digits = 2)), size = 4)

# 为 XGB 模型创建 KM 图
fit_xgb <- survfit(Surv(time, hospital_expire_flag) ~ XGB_group, data = ds_combined)
xgb_diff <- survdiff(Surv(time, hospital_expire_flag) ~ XGB_group, data = ds_combined)
xgb_chisq <- round(xgb_diff$chisq, 2)
xgb_plot <- ggsurvplot(fit_xgb,
           data = ds_combined,
           pval = F,
           pval.method = F,
           conf.int = TRUE,
           risk.table = TRUE,
           title = "XGBoost",
           legend.labs = c("Low Risk", "High Risk"),
           palette = c("blue", "red"),
           risk.table.height = 0.25,
           xlab = "Time (days)",
           ylab = "Survival probability")
xgb_plot$plot <- xgb_plot$plot + annotate("text", x = max(ds_combined$time)/4, y = 0.2, 
                                        label = paste("Log-rank χ² =", round(xgb_diff$chisq, 2), "\nP-value =", format.pval(xgb_diff$pvalue, digits = 2)), size = 4)



# 将四个图拼接在一起
arrange_plots <- grid.arrange(
  lr_plot$plot, rf_plot$plot, 
  lgbm_plot$plot, xgb_plot$plot, 
  ncol = 2, nrow = 2
)


# 保存图形为PNG文件
png("survival_curves.png", width = 1200, height = 1200, res = 150)
arrange_plots <- grid.arrange(
  lr_plot$plot, rf_plot$plot, 
  lgbm_plot$plot, xgb_plot$plot, 
  ncol = 2, nrow = 2
)
dev.off()