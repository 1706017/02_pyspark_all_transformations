#Concept 1) Data Reading using spark reader api

df1 = spark.read.format("csv")\
           .option("inferSchema",True)\
           .option("header",True)\
           .option("path","/path/to/input/file")\
           .load()

########################################################
