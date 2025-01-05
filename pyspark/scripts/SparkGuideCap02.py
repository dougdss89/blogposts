from pyspark.sql import SparkSession
from pyspark.sql.functions import (window, column, desc, col)
spark = SparkSession.builder.appName("SparkGuide").getOrCreate()

staticDataFrame = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceGlobalTempView("retail_data")
staticSchema = staticDataFrame.schema

staticDataFrame\
    .selectExpr("CustomerId",
                "(UnitPrice * Quantity) as TotalCost",
                "InvoiceData")\
    .groupby(
        col("CustomerId"), window(col("InvoiceData"),"1 Day"))\
    .sum("TotalCost")\
    .show(5)
    
