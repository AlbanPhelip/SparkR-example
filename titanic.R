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
showDF(select(titanic,"name"))
head(select(titanic, titanic$name)) 

# Filter and select several columns 
rich <- filter(titanic, titanic$fare > 200)
head(select(rich, c(rich$fare, rich$name)))
count(rich)

# GroupBy
test <- groupBy(titanic, titanic$age)
age <- summarize(test, count = count(titanic$age))
showDF(age)

ageCollect <- collect(age)
wtd.hist(ageCollect$age, weight = ageCollect$count, breaks = 16, col="lightblue")

t <- collect(titanic)
hist(t$age, breaks = 16, col="lightblue")