# Set this to where Spark is installed
Sys.setenv(SPARK_HOME="/Users/albanphelip/Documents/Xebia/Spark/spark-1.5.0-SNAPSHOT-bin-hadoop2.6")
# This line loads SparkR from the installed directory
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
library(weights)
summarize <- SparkR::summarize

# Initialize SparkContext and SQLContext
sc <- sparkR.init(master="local[*]", appName="SparkR-DataFrame-example")

sqlContext <- sparkRSQL.init(sc)

# Get the data
titanic <- read.df(sqlContext, "/Users/albanphelip/Documents/Xebia/Articles/SparkR/SparkR-example/bigTitanic.json", "json")

# Schema
printSchema(titanic)

# First lines
showDF(titanic)

test <- select(titanic, "fare", "age", "survived", "pclass", "sex")

# GLM
#formule <- "survived ~ sex + fare + age + pclass"
#model <- SparkR::glm(fare ~ age, family = "gaussian", data = test)
model <- SparkR::glm(survived ~ age + fare + pclass + sex, family = "binomial", data = test)

prediction <- predict(model, newData = test)
prediction$diff <- (prediction$survived - prediction$prediction)^2

precision <- 1 - sum(collect(select(prediction, "diff")))/count(test)
precision
