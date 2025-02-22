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

#Concept3: To explicitly define schema of dataframe
from pyspark.sql.types import *
from pyspark.sql.functions import *

df_csv_schema = StructType([StructField("Item_Identifier",StringType(),True),\
                           StructField("Item_Weight",StringType(),True),\
                           StructField("Item_Fat_Content",StringType(),True),\
                           StructField("Item_Visibility",DoubleType(),True),\
                           StructField("Item_Type",StringType(),True),\
                           StructField("Item_MRP",DoubleType(),True),\
                           StructField("Outlet_Identifier",StringType(),True),\
                           StructField("Outlet_Establishment_Year",IntegerType(),True),\
                           StructField("Outlet_Size",StringType(),True),\
                           StructField("Outlet_Location_Type",StringType(),True),\
                           StructField("Outlet_Type",StringType(),True),\
                           StructField("Item_Outlet_Sales",DoubleType(),True)])

#Attaching the schema to the dataframe
df_csv_with_schema = spark.read.format("csv")\
                          .option("header",True)\
                          .schema(df_csv_schema)\
                          .option("mode","PERMISSIVE")\
                          .load("/FileStore/tables/BigMart_Sales.csv")
######################################################################################################







