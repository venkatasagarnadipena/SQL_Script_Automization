# Databricks notebook source
# MAGIC %md 
# MAGIC # Script for Automating Delta Table Creation
# MAGIC
# MAGIC ## Requirements:
# MAGIC
# MAGIC 1. **Column Names:** A list of column names.
# MAGIC 2. **Schema List:** The schema list of the columns provided in the column list.
# MAGIC 3. **Nullable List:** Specifies constraints for any column. If not applicable, provide an empty list.
# MAGIC 4. **Table Name:** The name of the table where the data will be stored.
# MAGIC 5. **Storage Account Path:** The path of the storage account where the data will be stored.
# MAGIC 6. **Partition List:** Specifies if any partition is required. If not, provide an empty list.
# MAGIC
# MAGIC ## Notes:
# MAGIC
# MAGIC 1. Always put the column names list, Schema list and Nullable list (whenever used) in same orders
# MAGIC 2. To assign a null parameter to any specific column, define a list of values based on the column list provided.
# MAGIC 3. If storing in built-in storage or Hive, no need to mount the storage. If storing in an external storage, configure the mount point and provide the complete path to store the data.
# MAGIC

# COMMAND ----------

# MAGIC %md ## Inputs for Automation

# COMMAND ----------

# Create table command inputs  
column_name=[]                                                  #Input for Target table columns in an array list with the required order
table_schema=[]                                                 #Input for Target table schema same as orders of Column list, in array format
nullable=[]                                                     #Input for Target table schema same as orders of Column list, in array format
target_table_name=""                                            #Input for Target table name which we want
path=''                                                         #Input for Target table path here we want to store our data 
partition_column=[]                                             #Input for Target table partition columns if required, in form of array 

# COMMAND ----------

# DBTITLE 1,Example parameter
# # Create table command inputs  
# column_name=['emp','id']                                                  #Input for Target table columns in an array list with the required order
# table_schema=['string','int']                                             #Input for Target table schema same as orders of Column list, in array format
# nullable=[]                                                               #Input for Target table schema same as orders of Column list, in array format
# target_table_name="abc"                                                   #Input for Target table name which we want
# path='dbfs:/FileStore/tables/abc'                                         #Input for Target table path here we want to store our data
# partition_column=[]                                                       #Input for Target table partition columns if required, in form of array

# COMMAND ----------

# MAGIC %md ## Create Table command automation in Sql

# COMMAND ----------

# DBTITLE 1,If we are sure Null array wont be empty at any point if time 
b=''
p=''
max_len=len(column_name)
pmax_len=len(partition_column)

if(len(column_name)==len(table_schema)==len(nullable)):              #Checks if the All 3 list have prper values or not
    for i in range(0,len(column_name)):
        b+=column_name[i]+' '+table_schema[i]
        if(i!=max_len-1):                                            #Used to remove the end ',' post the column and schema declaration
            if(nullable[i]==''):                                     #Used to identify the nullable and non nullable columns 
                b+=", \n"
            else:
                b+=" "+nullable[i]+', \n'
        else:
            if(nullable[i]==''):
                b+=" \n"
            else:
                b+=" "+nullable[i]+' \n'
    if (len(partition_column)!=0):                                   #Used to identify if we have to do partition or not 
        for i in range(len(partition_column)):
                if(i!=pmax_len-1): 
                    p+=partition_column[i]+","
                else:
                    p+=partition_column[i]
        print("%sql\n CREATE TABLE IF NOT EXISTS "+target_table_name,"\n ( \n"+b+") \n USING DELTA \n LOCATION ","'"+path+"'"\
        ,"PARTITIONED BY ("+p+")","\n TBLPROPERTIES (delta.autoOptimize.autoCompact=true,delta.autoOptimize.optimizeWrite=true)")
    else:
        print("%sql\n CREATE TABLE IF NOT EXISTS "+target_table_name,"\n ( \n"+b+") \n USING DELTA \n LOCATION ","'"+path+"'"\
        ,"\n TBLPROPERTIES (delta.autoOptimize.autoCompact=true,delta.autoOptimize.optimizeWrite=true)")
else:
    print("Need proper inputs as one of the list is having less size \n","Column name list-",len(column_name),"\n schema list-",len(table_schema),"\n Nullable-",len(nullable))

# COMMAND ----------

# DBTITLE 1,If we are sure Null array is empty at any point if time
b=''
p=''
max_len=len(column_name)
pmax_len=len(partition_column)

if(len(column_name)==len(table_schema)):                           #Checks if the All 3 list have prper values or not
    for i in range(0,len(column_name)):
        b+=column_name[i]+' '+table_schema[i]
        if(not nullable):                                          #Used to identify the nullable array is empty otr not as it define the primary key set  either it should be empty or it should be full array for all columns 
            if(i!=max_len-1):                                      #Used to remove the end ',' post the column and schema declaration
                b+=", \n"
            else:
                b+=" \n"
        else:
            if(i!=max_len-1):
                if(nullable[i]==''):                               #Used to identify the nullable and non nullable columns
                    b+=", \n"
                else:
                    b+=" "+nullable[i]+', \n'
            else:
                if(nullable[i]==''):
                    b+=" \n"
                else:
                    b+=" "+nullable[i]+' \n'
    if (len(partition_column)!=0):                                 #Used to identify if we have to do partition or not 
        for i in range(len(partition_column)):
                if(i!=pmax_len-1): 
                    p+=partition_column[i]+","
                else:
                    p+=partition_column[i]
        print("%sql\n CREATE TABLE IF NOT EXISTS "+target_table_name,"\n ( \n"+b+") \n USING DELTA \n LOCATION ","'"+path+"'"\
        ,"PARTITIONED BY ("+p+")","\n TBLPROPERTIES (delta.autoOptimize.autoCompact=true,delta.autoOptimize.optimizeWrite=true)")
    else:
        print("%%sql\n CREATE TABLE IF NOT EXISTS "+target_table_name,"\n ( \n"+b+") \n USING DELTA \n LOCATION ","'"+path+"'"\
        ,"\n TBLPROPERTIES (delta.autoOptimize.autoCompact=true,delta.autoOptimize.optimizeWrite=true)")
else:
    print("Need proper inputs as one of the list is having less size \n","Column name list-",len(column_name),"\n schema list-",len(table_schema))

# COMMAND ----------

# MAGIC %md ## Create table command automation in Pyspark

# COMMAND ----------

b=''
p=''
max_len=len(column_name)
pmax_len=len(partition_column)


if(not nullable):
    print("Null array is empty so no code will be generated in the final query \n \n")
else:
    for i in range(len(nullable)):
        if(nullable[i]=="" or nullable[i]=="null" or nullable[i]==" " or nullable[i]=="Null" or nullable[i]=="NULL"):
            nullable[i]="True"
        else:
            nullable[i]="False"
if(len(column_name)==len(table_schema)):                            #Checks if the All 3 list have prper values or not
    for i in range(len(column_name)):
        b+='.addColumn("'+column_name[i]+'",dataType="'+table_schema[i]+'"'
        if(not nullable):                                           #Used to identify the nullable array is empty otr not as it define the primary key set  either it should be empty or it should be full array for all columns 
            b+=', comment="no specific comments for the Column")'
        else:
            b+=',nullable= '+nullable[i]+', comment="no specific comments for the Column")'
    if (len(partition_column)!=0):                                  #Used to identify if we have to do partition or not 
        for i in range(len(partition_column)):
                if(i!=pmax_len-1): 
                    p+='"'+partition_column[i]+'",'
                else:
                    p+='"'+partition_column[i]+'"'
        print('from delta.tables import *\n \n'+'DeltaTable.createIfNotExists(spark)'+'.tableName("'+target_table_name+'")'+b+".partitionedBy("+p+")"+'.location("'+path+'")'+'.execute()' )
    else:
        print('from delta.tables import *\n \n'+'DeltaTable.createIfNotExists(spark)'+'.tableName("'+target_table_name+'")'+b+'.location("'+path+'")'+'.execute()' )
else:
    print("Need proper inputs as one of the list is having less size \n","Column name list-",len(column_name),"\n schema list-",len(table_schema))
