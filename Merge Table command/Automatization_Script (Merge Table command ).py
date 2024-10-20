# Databricks notebook source
s_table="cba"
t_table="abc"
s_colunm=['emp','id']
t_column=['emp','id']
s_pk_list=['id','emp']
t_pk_list=['id','emp']

# COMMAND ----------

# DBTITLE 1,Merge command using SQL with no delectation of records command (the records deleted in source will not be deleted by this in target )
update_list=''
insert_list=''
values_list=''
if(len(s_colunm)==len(t_column)):
    pk_list=''
    if (len(s_pk_list)==len(t_pk_list)):#Pk start
        max_pk=len(s_pk_list)
        for i in range (0,len(s_pk_list)):
            if(i!=max_pk-1):
                pk_list+='target.'+s_pk_list[i]+' = source.'+t_pk_list[i]+' AND\n'

            else: 
                pk_list+='target.'+s_pk_list[i]+' = source.'+t_pk_list[i]+'\n'
        # print(pk_list)
    else:
        print('Primary ky list didnt matched')#pk end

    max_u=len(t_column)-1
    for i in range(len(t_column)):
        if(i!=max_u):
            update_list+='target.'+t_column[i]+'  =  source.'+s_colunm[i]+',\n'
            insert_list+=t_column[i]+','
            values_list+='source.'+t_column[i]+','
        else:
            update_list+='    target.'+t_column[i]+'  =  source.'+s_colunm[i]
            insert_list+=t_column[i]
            values_list+='source.'+t_column[i]
    # print(insert_list)
    print('%sql \n\nMERGE INTO '+t_table+' target\n'+'USING '+s_table+' source\n'+'ON '+pk_list+'\n'+'WHEN MATCHED THEN UPDATE \n'+'SET '+update_list+'\n\nWHEN NOT MATCHED THEN\nINSERT ('+insert_list+')\nVALUES ('+values_list+')')    
else:
    print("Provided coulumns set didnnt match for source and target")

# COMMAND ----------

# DBTITLE 1,Merge command using SQL with no delectation of records command (the records deleted in source will be deleted by this in target)
update_list=''
insert_list=''
values_list=''
if(len(s_colunm)==len(t_column)):
    pk_list=''
    if (len(s_pk_list)==len(t_pk_list)):#Pk start
        max_pk=len(s_pk_list)
        for i in range (0,len(s_pk_list)):
            if(i!=max_pk-1):
                pk_list+='target.'+s_pk_list[i]+' = source.'+t_pk_list[i]+' AND\n'

            else: 
                pk_list+='target.'+s_pk_list[i]+' = source.'+t_pk_list[i]+'\n'
        # print(pk_list)
    else:
        print('Primary ky list didnt matched')#pk end

    max_u=len(t_column)-1
    for i in range(len(t_column)):
        if(i!=max_u):
            update_list+='target.'+t_column[i]+'  =  source.'+s_colunm[i]+',\n'
            insert_list+=t_column[i]+','
            values_list+='source.'+t_column[i]+','
        else:
            update_list+='    target.'+t_column[i]+'  =  source.'+s_colunm[i]
            insert_list+=t_column[i]
            values_list+='source.'+t_column[i]
    # print(insert_list)
    print('%sql \n\nMERGE INTO '+t_table+' target\n'+'USING '+s_table+' source\n'+'ON '+pk_list+'\n'+'WHEN MATCHED THEN UPDATE \n'+'SET '+update_list+'\n\nWHEN NOT MATCHED THEN\nINSERT ('+insert_list+')\nVALUES ('+values_list+')\n'+'WHEN NOT MATCHED BY source\nTHEN DELETE ')    
else:
    print("Provided coulumns set didnnt match for source and target")
