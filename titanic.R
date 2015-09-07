# Set this to where Spark is installed
Sys.setenv(SPARK_HOME="/Users/albanphelip/Documents/Xebia/Spark/spark-1.4.1-bin-hadoop2.6")

# This line loads SparkR from the installed directory
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
library(weights)
summarize <- SparkR::summarize

# Initialize SparkContext and SQLContext
sc <- sparkR.init(master="local[*]", appName="SparkR-DataFrame-example", sparkPackages="com.databricks:spark-csv_2.11:1.0.3")

sqlContext <- sparkRSQL.init(sc)

# Get the data
titanic <- read.df(sqlContext, path="./Documents/Xebia/Articles/SparkR/SparkR-example/data_titanic.csv", source="com.databricks.spark.csv",header="true")

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

t <- collect(titanic)
hist(t$age, breaks = 16, col="lightblue")

# Pipeline
library(magrittr)
groupBy(titanic, titanic$age) %>% summarize(., count = count(titanic$age)) %>% showDF(., 5)
