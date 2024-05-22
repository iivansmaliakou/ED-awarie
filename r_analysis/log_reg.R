# load dataframe
df_train <- read.csv("../data/4_fill_nans->one_hot->merge/processed_data_train_classification.csv")

# create logistic regression model
logistic_model <- glm(var1 ~ var2, data=df_train, family=binomial)

#Data frame with hp in ascending order
Predicted_data <- data.frame(var2=seq(
  min(df$var2), max(df$var2),len=500))

# Fill predicted values using regression model
Predicted_data$var1 = predict(
  logistic_model, Predicted_data, type="response")

# Plot Predicted data and original data points
plot(var1 ~ var2, data=df)
lines(var1 ~ var2, Predicted_data, lwd=2, col="green")