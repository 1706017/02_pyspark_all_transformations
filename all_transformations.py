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

Concept4: .select() Transformation

#case1
df_csv_with_schema.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

#case2
df_csv_with_schema.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()
######################################################################################################
Concept5: .alias() Transformation

df_csv_with_schema.select(col('Item_Identifier').alias('Item_Id')).display()
######################################################################################################
Concept6: .filter() Transformation


#Scenario -01 to fetch only those records where item_fat_content is Regular

df_csv_with_schema.filter(col('Item_Fat_Content')=='Regular').display()

#Scenario-02 To fetch those records only where item type is soft drinks and Item weight is less than 10
df_csv_with_schema.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10) ).display()


#Scenario3 : To fetch all the records where outlet size is null and outlet tier is in tier1,tier2
df_csv_with_schema.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

########################################################################################################
Concept7:  .withColumnRenamed() Transformation
It is used to change the column name


df_csv_with_schema.withColumnRenamed('Item_Weight','Item_WT').display()

#########################################################################################################
Concept8: Use of .withColumn() Trnsformation

Use1: To add a new column
Use2 : To update the preexisting column
Use3 : To create a new column by doing some computation with the help of existing columns


#scenario1: 
df_csv_with_schema.withColumn('flag',lit('new')).display()


#Scenario2:
df_csv_with_schema.withColumn('Multiply',col('Item_MRP')*col('Item_Outlet_Sales')).display()

#Scenario3:
df_csv_with_schema.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
                  .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF')).display()

#################################################################################################################################

Concept9: Type cast in pyspark using .cast() Transformation


df_csv_with_schema.withColumn('Item_Weight',col('Item_Weight').cast(IntegerType())).printSchema()

####################################################################################################################################
Concept10:  .sort() Transformation


#Scenario1:
df_csv_with_schema.sort(col('Item_Weight').desc()).display()

#Scenario2:
df_csv_with_schema.sort(col('Item_Weight').asc()).display()

#Scenario3: Sorting on Descending Order for multiple columns
df_csv_with_schema.sort(['Item_Weight','Item_MRP'],ascending=[0,0]).display()

####################################################################################################################################
Concept 11: .limit() Transformation

To get only specified number of records from dataframe we use .limit() 


df_csv_with_schema.limit(5).display()
####################################################################################################################################
Concept 12 : Use of .drop() Transformation


df_csv_with_schema.drop('Item_Visibility','Item_Weight').display()
###################################################################################################################################
Concept 13: Use of .dropDuplicates() Transformation

Use to drop duplcate records based on all columns or some specific columns

 df_csv_with_schema.dropDuplicates()
 df_csv_with_schema.dropDuplicates(['Item_Weight','Item_Visibility'])

#####################################################################################################################################

Concept 14: use of current_date() methods


df_csv_with_schema.withColumn('curr_date',current_date()).display()
####################################################################################################################################
Concept 15: Use of date_format() Transformation

df_new.withColumn("Current_date",date_format("Current_date","dd-MM-yyyy")).display()
###################################################################################################################################

Concept 16: Handling Null 

Here we have two strategies 
 a) Dropping nulls
 b) Filling nulls

Dropping Nulls:
 Scenario1) df.dropna('all').display  -> this will drop any record having all null in the column
 Scenario2) df.dropna('any').display -> This will drop any record having null in any of the column
 Scenario3) df.dropna(subset = ['Outlet_Size']).display() -> this will drop those records having null in the specifc column

Fill Nulls:
 Scenario1: df.fillna('NA').display()
 Scenario2: df.fillna('NA',subset = ['Outlet_Size']).display()
####################################################################################################################################
Date: 27th Feb 2025 

Concept 17: Split and Indexing Transformation
split and indexing syntax:
 df.withColumn('col_name',split('col_name',' ')[1])
 #here we have given space as our delimiter

#############################################################################################################################################################################
Concept 18: Use of explode() transformation 


 df.withColumn('col_name',explode('col_name')).display()

•  Basically the explode() function simply explode the list into new rows 


 

df_new_v1.withColumn('Outlet_type_exploded',explode('Outlet_type_v1')).limit(5).display()

#############################################################################################################################################################################
Concept 19: Use of Group By Transformation 

df_new.groupBy('Item_Type').agg(sum('Item_MRP')).display()
 

df_new.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

