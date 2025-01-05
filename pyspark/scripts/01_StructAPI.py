from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    
#criando a session
    spark = SparkSession.builder.appName('SparkProg').getOrCreate()
            
#create schema
schemaSpark = StructType([StructField('Produto', StringType(), False),\
                            StructField('Categoria', StringType(), True),\
                            StructField('Valor', IntegerType() , False)])

#criando o dataframe
sparkDf = spark.createDataFrame([('A', 'Alimento', 30),\
                                        ('B', 'Papelaria', 40),\
                                        ('C', 'Informatica', 30),\
                                        ('D', 'Papelaria', 40),\
                                        ('E', 'Alimento', 40),\
                                        ('F', 'Informatica', 100),\
                                        ('G', 'Papelaria', 80),\
                                        ('A', 'Alimento', 30),\
                                        ('D', 'Papelaria', 50),\
                                        ('C', 'Informatica', 50),\
                                        ('J', 'Bebida', 100)],
                                    ['Produto', 'Categoria', 'Valor'], schemaSpark)

#agregando
aggregateCategory = sparkDf.groupby('Categoria').sum('Valor')
aggregateCategory.show()

