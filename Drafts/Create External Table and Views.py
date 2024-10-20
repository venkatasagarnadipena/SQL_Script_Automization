# Databricks notebook source
schema='bronze'
table_name='Controltable'
adls_source='adls'
path='bsvbs/acbac'
read_format='patvsasv'
column_list=['Customer ID','Project Key','Project Start','Project End']
table_schema_list=['INT','STring','String','Date']
table_schema_list=[x.upper() for x in table_schema_list ]
table_schema_list = ['VARCHAR(255)' if x == 'STRING' else x for x in table_schema_list]
table_schema_list = ['TIMESTAMP' if x == 'TIMESTAMP' or x=='DATETIME' else x for x in table_schema_list]
table_schema_list = ['DATE' if x == 'DATE' else x for x in table_schema_list]
table_schema_list = ['DECIMAL(10,4)' if x == 'DECIMAL' or x=='FLOAT' else x for x in table_schema_list]
table_schema_list = ['INTEGER' if x == 'INT' else x for x in table_schema_list]

# COMMAND ----------

len_column_list=len(column_list)-1
temp=''
for i in range(len(column_list)-1):
    temp+='['+column_list[i]+']  ['+table_schema_list[i]+'],\n'
temp+='['+column_list[len_column_list]+']  ['+table_schema_list[len_column_list]+']\n'
print('CREATE EXTERNAL TABLE ['+schema+'].['+table_name+'] ( \n',temp+')\nWITH (DATA_SOURCE = ['+adls_source+"], LOCATION = N'"+path+"',FILE_FORMAT = ["+read_format+'])\nGO')
