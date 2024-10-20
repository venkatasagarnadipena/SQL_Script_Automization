# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format('csv').option("header",True).load('dbfs:/FileStore/tables/createtable.csv')
df.show()

# COMMAND ----------



# COMMAND ----------

column_name=df.select('table_columns').rdd.map(lambda row: row[0]).collect()[1]
column_name=column_name.split(',')
table_schema=df.select('table_schema').rdd.map(lambda row: row[0]).collect()[1]
table_schema=table_schema.split(',')
nullable=df.select('null_factor').rdd.map(lambda row: row[0]).collect()[1]
nullable=nullable.split(',')

# COMMAND ----------

t_struc=[]
t_null=[]
for i in range(len(table_schema)):
    if(table_schema[i]=='string' or table_schema[i]=='String'):
        t_struc.append('StringType')
    elif(table_schema[i]=="timestamp" or table_schema[i]=='TimeStamp' ):
            t_struc.append('TimestampType')
    elif(table_schema[i]=="Integer" or table_schema[i]=="integer"):
        t_struc.append('IntegerType')
    elif(table_schema[i]=="date" or table_schema[i]=="Date"):
        t_struc.append('DateType')
    elif(table_schema[i]=="float" or table_schema[i]=="Float"):
        t_struc.append('FloatType')
    elif(table_schema[i]=="decimal" or table_schema[i]=="Decimal"):
        t_struc.append('StringType')
    elif(table_schema[i]=="boolean" or table_schema[i]=="Boolean"):
        t_struc.append('BooleanType')
for i in range(len(nullable)):
    if(nullable[i]=='null'or nullable[i]=='Null'):
        t_null.append('True')
    else:
        t_null.append('False')
if(len(column_name)==len(table_schema)==len(nullable)):
    strc=''
    for i in range(len(column_name)):
        strc+='    StructField("'+column_name[i]+'",'+t_struc[i]+'(),'+t_null[i]+'), \\\n'
    print('StructType([ \\\n',strc,'])')
