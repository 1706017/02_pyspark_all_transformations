#Concept 1) Data Reading using spark reader api

df1 = spark.read.format("csv")\
           .option("inferSchema",True)\
           .option("header",True)\
           .option("path","/path/to/input/file")\
           .load()

#displaying the dataframe the we have read
 df1.display()

########################################################

#Concept2: Reading JSON file format data

 df_json = spark.read.format("json")\
                            .option("multiline",False)\
                            .option("inferSchema",True)\
                            .option("path","path/to/json/file")\
                            .load()
########################################################



