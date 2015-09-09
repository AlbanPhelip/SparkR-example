# Set this to where Spark is installed
Sys.setenv(SPARK_HOME="/Users/albanphelip/Documents/Xebia/Spark/spark-1.5.0-bin-hadoop2.6")
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.2.0" "sparkr-shell"')

# This line loads SparkR from the installed directory
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
library(weights)
summarize <- SparkR::summarize

# Initialize SparkContext and SQLContext
sc <- sparkR.init(master="local[*]", appName="SparkR-DataFrame-example")

sqlContext <- sparkRSQL.init(sc)

# Get the data
titanic <- read.df(sqlContext, path="./Documents/Xebia/Articles/SparkR/SparkR-example/data_titanic.csv", source="com.databricks.spark.csv", header="true")

# Schema
printSchema(titanic)

# First lines
showDF(titanic)

# Change types
titanic$age <- cast(titanic$age, "double")
titanic$fare <- cast(titanic$fare, "double")
titanic$sibsp <- cast(titanic$sibsp, "double")
titanic$parch <- cast(titanic$parch, "double")
titanic$body <- cast(titanic$body, "double")
titanic$pclass <- cast(titanic$pclass, "long")
titanic$survived <- cast(titanic$survived, "long")

printSchema(titanic)

# Select one column
name <- select(titanic, titanic$name)
showDF(name, 5)

# Filter and select several columns 
rich <- filter(titanic, titanic$fare > 200)
head(select(rich, c(rich$fare, rich$name)))
showDF(select(rich, c(rich$fare, rich$name)), 5)

# Number of people who paid their ticket more than 200$
count(rich)

# GroupBy
groupByAge <- groupBy(titanic, titanic$age)
age <- summarize(groupByAge, count = count(titanic$age))
showDF(age, 5)

ageCollect <- collect(age)
wtd.hist(ageCollect$age, weight = ageCollect$count, breaks = 16, col="lightblue", main = "Répartition des individus en fonction de l'âge", xlab = "Age")

# Pipeline
library(magrittr)
groupBy(titanic, titanic$age) %>% summarize(., count = count(titanic$age)) %>% showDF(., 5)


# MLlib
# Get the important variables
dataForGLM <- select(titanic, "fare", "age", "survived", "pclass", "sex")

# Fill the null values
dataWithoutNull <- dataForGLM %>% fillna(., 28, "age") %>% fillna(. , 14.45, "fare")

# Building the model
model <- SparkR::glm(survived ~ sex + age + fare + pclass, family = "binomial", data = dataWithoutNull)

# Make the prediction
prediction <- predict(model, newData = dataWithoutNull)

# Create the variable diff : 0 if bad prediction, 1 if good prediction 
prediction$diff <- (prediction$survived - prediction$prediction)^2

# Compute the mean of 'diff' : comute the percentage of good prediction
precision <- 1 - sum(collect(select(prediction, "diff")))/count(dataWithoutNull)
precision

