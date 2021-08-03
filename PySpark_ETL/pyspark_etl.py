#Clear the File Landing Directory in DBFS(Databricks File System) 
dbutils.fs.rm('/FileStore/tables',True)

#Create SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,count
spark = SparkSession.builder.appName('ETL Pipeline').getOrCreate()

#Extract

#Upload the dataset in DBFS
df = spark.read.text('/FileStore/tables/WordData.txt')

display(df)  	#Display is a Databricks function to view the dataframe. Provides a better view of dataset than show() action.


#Transformation

#Convert String of data into list of words and perform word count
# 1. Split and put it in a list 
df2 = df.withColumn('splitedData',split('value'," "))

# 2. Separate the list of words
df3 = df2.withColumn("words",explode('splitedData'))

# 3.Perform word count
wordCount = df3.groupBy('words').agg(count('*').alias('count'))

display(wordCount)

#Load

# 1. Create RDS database instance of PostgreSQL
# 2. Link the RDS PostgreSQL instance with pgAdmin4 in local.(endpoint and port will be provided in RDS-database)
# 3. Run below code to save the dataset in RDS instance

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://<database-endpoint>/"
table = "pyspark_wordcount.WordCount"
user = "<RDS username>"
password = "<RDS password>"

wordCount.write.format('jdbc').option("driver",driver) .option("url",url) .option("dbtable",table).option("mode","append").option("user",user).option("password",password).save()

# 4. View the dataset in local pgAdmin4 (select * from pyspark_wordcount.WordCount)