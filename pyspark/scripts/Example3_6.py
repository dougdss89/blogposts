#criando o mesmo schema utilizando a linguagem SQL

from pyspark.sql import SparkSession
ddlSchema = "`Id` INT, `First` STRING, `Last` STRING,`Url` STRING,\
            `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
            
#create data
data = [[1, "Jules", "Damji", "https://tinyurl.1", '01/04/2016', 4535, ["twitter", "LinkedIN"]],
        [2, "Brooke", "Wenig", "https://tinyurl.2", "06/07/2019", 8908, ["twitter", "LinkedIN"]],
        [3, "Denny", "Lee", "https://tinyurl.3", '05/05/2018', 7659, ["twitter", "LinkedIN", "web", "Facebook", "LinkedIN"]],
        [4, "Tathagat", "Das", "https://tinyurl.4", '05/12/2018', 10568, ["twitter", "LinkedIN", "web", "Facebook", "LinkedIN"]],
        [5, "Matei", "Zaharia", "https://tinyurl.5", '05/14/2014', 40578, ["twitter", "LinkedIN", "web", "Facebook", "LinkedIN"]]]

#main program
if __name__ == "__main__":
    #criando a sessão
    
    spark = (SparkSession\
        .builder\
        .appName("Example3_6.py")\
        .getOrCreate())
    
#create dataframe utilizando o schema acima
blogsDF = spark.createDataFrame(data, ddlSchema)

#aqui irá mostrar o dataframe
blogsDF.show()

print(blogsDF.printSchema())