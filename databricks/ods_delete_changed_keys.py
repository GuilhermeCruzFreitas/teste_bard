# Databricks notebook source
# MAGIC %md
# MAGIC #Essa função foi criada pois em nossas bases de origem, de vez em quando, ocorrem modificações nas chaves primárias. Quando isto ocorre, o processo de atualização de cargas incrementais realiza um insert do novo valor, ao invés de atualizar o valor antigo, porque o union é baseado nas chaves. Tendo em vista que isto pode acarretar em problemas de homologação, precisamos identificar este registro antigo e removê-lo

# COMMAND ----------

from pyspark.sql.functions import from_json, col, explode, max,monotonically_increasing_id,row_number,lower,conv,bin,reverse
from delta.tables import *
from datetime import datetime,date,timedelta,timezone
from pyspark.sql.window import Window


# COMMAND ----------

datalake = "___dataLakeName___"
if "dataLakeName" in datalake:
  datalake = 'dlsanalyticsdsv'
  
env = "___ambiente___"
if "ambiente" in env:
  env = 'dsv'
  

# COMMAND ----------

# MAGIC %run /general/functions/fnc_retorna_path_containers

# COMMAND ----------

datalake_path = fnc_retorna_path_containers(datalake)

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("keys", "")
dbutils.widgets.text("ods_database", "")
dbutils.widgets.text("owner", "")
dbutils.widgets.text("produto", "")
dbutils.widgets.text("banco", "")

# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
owner = dbutils.widgets.get("owner")
ods_database = dbutils.widgets.get("ods_database")
produto = dbutils.widgets.get("produto")
banco = dbutils.widgets.get("banco")
keys = dbutils.widgets.get("keys")


# COMMAND ----------

# MAGIC %run /general/functions/fnc_existencia_arquivo

# COMMAND ----------

def get_dfm_full(produto,banco,owner,table_name,datalake_path):
  pathFull=datalake_path['landed']+"attunity/"+produto+"/"+banco+"/"+owner+"."+table_name
  #pegar o ultimo dfm do repositoriaxxx
  path_files_json_dfm_full = sorted(dbutils.fs.ls(pathFull))
  #print(path_files_json_dfm_full)

  if not path_files_json_dfm_full:
    dbutils.notebook.exit("Sem informacaoo")

  #pegar o ultimo dfm do repositorio
  dfm_file_full = path_files_json_dfm_full[-2][0]
  if(".dfm" not in dfm_file_full):
    dbutils.notebook.exit("Sem dfm full para criacao do schema")
  return dfm_file_full

# COMMAND ----------

def get_dfm_incremental(produto,banco,owner,table_name,datalake_path):
  dfm_file = None
  pathIncremental = datalake_path['landed']+"attunity/"+produto+"/"+banco+"/"+owner+"."+table_name+"__ct"
  #pegar a ultima particao do repositorio incremental
  print(pathIncremental)
  path_last_incremental = sorted(dbutils.fs.ls(pathIncremental))
  print(path_last_incremental)
  print(type(path_last_incremental))
  if(len(path_last_incremental)>0):
    path_files_incremental = path_last_incremental[-1][0]
    path_files_json_dfm_incremental = sorted(dbutils.fs.ls(path_files_incremental))
    #print(path_files_json_dfm_incremental)
    if not path_files_json_dfm_incremental:
      #dbutils.notebook.exit("Sem informacao")
      return dfm_file
    #pegar o ultimo dfm do repositorio
    dfm_file_incremental = path_files_json_dfm_incremental[-2][0]
    if(".dfm" in dfm_file_incremental):
      dfm_file = dfm_file_incremental
  return dfm_file

# COMMAND ----------

@udf("int")
def hex_to_int(x):
  
  scale = 16 ## equals to hexadecimal

  return (int(x, scale))

# COMMAND ----------

def remove_changed_keys(incremental_df,keys,datalake_path,produto,banco,owner,table_name):
  print("Teste de remocao incremental")
  sub_path = datalake_path['landed']+"attunity/"+produto.lower()+"/"+banco.lower()+"/"
  path = sub_path+""+owner+"."+table_name

  dir_list = dbutils.fs.ls(path + "__ct/")
  dir_names = []
  for i in dir_list:
    dir_names.append(i.name[:8])

  if not dir_names:
    return (incremental_df.limit(0),incremental_df)
  
  if incremental_df.filter(incremental_df['header__change_oper']!='D').count() == 0:
    return (incremental_df.limit(0),incremental_df)
  
  dfm_file = get_dfm_incremental(produto,banco,owner,table_name,datalake_path)

  if dfm_file == None:
    print('Utilizando dfm full')
    dfm_file = get_dfm_full(produto,banco,owner,table_name,datalake_path)

  df_dfm = (spark
                          .read
                          .format("json")
                          .load(dfm_file, multiLine=True)
            )

  df_dfm_exploded = (df_dfm.select(explode(df_dfm.dataInfo.columns))
                           .select(col('col.name'),col('col.ordinal'),col('col.primaryKeyPos'))
                           .withColumn('name', lower(col('name')))
                           .withColumnRenamed('name','col_name')
                    )
  
  incremental_df.createOrReplaceTempView('vw_incremental')
  df_columns = spark.sql("describe table vw_incremental").select(lower(col('col_name')).alias('col_name'))
  df_columns = df_columns.join(df_dfm_exploded, ['col_name'], 'inner')
  
  df_dfm_colnames = df_columns.select(col('col_name'))
  headers_col =  df_dfm_colnames.filter(df_dfm_colnames.col_name.like('header__%'))
  



  ##criando indice
  #df_columns = df_columns.filter(~df_dfm_colnames.col_name.like('header__%')).withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())) )
  df_columns = df_columns.withColumnRenamed('ordinal', 'id')
  list_keys = keys.lower().split(',')



  list_dfm_key_ordinals = df_columns.select("id").filter(col("col_name").isin(list_keys)).rdd.flatMap(lambda x: x).collect()


  list_dfm_ordinals = df_columns.select(col('id')).rdd.flatMap(lambda x: x).collect()[:]
  list_dfm_ordinals.sort()
  max_ordinal = list_dfm_ordinals[-1]
  num_bits = len(format(int(incremental_df.filter(incremental_df['header__change_oper']!='D').select(max("header__change_mask")).collect()[0][0],16),'b'))
  
  #trtamento quando a tabela tem mais de 64 bits
  if(max_ordinal > num_bits  ):
    max_ordinal = num_bits
    
    
  table_key_mask = '0'* num_bits
  table_key_mask = list(table_key_mask[:])

  list_dfm_key_ordinals = [num for num in list_dfm_key_ordinals if num < max_ordinal]

  for i in list_dfm_key_ordinals:
    table_key_mask[max_ordinal - i] = '1'

  table_key_mask = "".join(table_key_mask[::])

  decimal_key_bitwise = int(table_key_mask, 2)
  
  
  
  incremental_df_updates = incremental_df.select("*").where(incremental_df.header__change_mask != "").where(incremental_df.header__change_oper == "U")
  change_mask_df = incremental_df_updates.select("*", conv("header__change_mask",16,10).cast('long').alias("bitwise_int"))


  change_mask_df_key_updated = change_mask_df.select('*').where(change_mask_df["bitwise_int"].bitwiseAND(decimal_key_bitwise) > 0)

  if(change_mask_df_key_updated.rdd.isEmpty()):
    print("Nao tem mudanca de chave")
    return(incremental_df.limit(0),incremental_df)
  
  
  ta = change_mask_df_key_updated.alias('ta')
  tb = incremental_df.alias('tb')

  tc = ta.join(tb,ta.header__change_seq == tb.header__change_seq).select("tb.*")
  tc = tc.select("*").where(tc.header__change_oper == "B")
  
  tc_tempview_name = "before_images_to_delete"
  tc.createOrReplaceTempView(tc_tempview_name)
  
  incremental_df_tempview_name = "incremental_df"
  incremental_df.createOrReplaceTempView(incremental_df_tempview_name)
  
  where_string = ""
  key_list = keys.split(",")

  incremental_df_alias = 'TGT'
  before_images_to_delete_alias = 'SRC'

  for chave in key_list:
    where_string = where_string + "{0}.{2} = {1}.{2} and ".format(incremental_df_alias,\
                                                                  before_images_to_delete_alias,\
                                                                  chave)


  where_string = where_string + " to_timestamp(ifnull({0}.header__timestamp,{0}.data_carga_utc)) <= to_timestamp(ifnull({1}.header__timestamp,{1}.data_carga_utc)) and ifnull({0}.header__change_seq,0) < ifnull({1}.header__change_seq,0) ".format(incremental_df_alias,\
                                                                  before_images_to_delete_alias)
  
  delete_string = "select {1}.* from {0} {1} left join {2} {3} on {4} where {3}.{5} is null".format(incremental_df_tempview_name,\
                                                                                              incremental_df_alias,\
                                                                                              tc_tempview_name,\
                                                                                              before_images_to_delete_alias,\
                                                                                              where_string,\
                                                                                               key_list[0])
  df_final = spark.sql(delete_string)
  
  
  return  tc,df_final

# COMMAND ----------


