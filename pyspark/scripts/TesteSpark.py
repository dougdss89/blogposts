        
#sempre que for trabalhar com dataframe, declare o schema na construção
#isso irá poupar o Spark de trabalho extra.

#declarando schema na criação de um dataframe
#aqui estamos utilizando linguagem de programação
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql 

schema = StructType([StructField("Id", IntegerType(), False),
                    StructField("Author", StringType(), False),
                    StructField("title", StringType(), False),
                    StructField("pages", IntegerType(), False)])

createData = [[1, 'George Martin', 'Game of Thrones', 1000],
              [2, 'Stephen King', 'The Dark Tower', 2000],
              [3, 'Jordan B. Peterson', '12 Rules for Life', 500],
              [4, 'J.K Rowling', 'Harry Potter', 1500]]

if __name__ == "__main__":
        
        spark = (SparkSession\
                .builder\
                .appName("TesteSpark.py")\
                .getOrCreate())
        
#create dataframe
authorDataFrame = spark.createDataFrame(createData, schema) 

authorDataFrame.show()
print(authorDataFrame.printSchema())