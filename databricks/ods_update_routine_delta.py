# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime,date,timedelta,timezone
from pyspark.sql.functions import coalesce,max
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /management/ods_tables/functions/ods_delete_changed_keys

# COMMAND ----------

spark = (SparkSession
          .builder
          .appName("update de ods's")
          .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
          .getOrCreate())

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("keys", "")
dbutils.widgets.text("ods_database", "")
dbutils.widgets.text("owner", "")
dbutils.widgets.text("produto", "")
dbutils.widgets.text("banco", "")


# COMMAND ----------

table_name = dbutils.widgets.get("table_name")
keys = dbutils.widgets.get("keys")
owner = dbutils.widgets.get("owner")
ods_database = dbutils.widgets.get("ods_database")
produto = dbutils.widgets.get("produto")
banco = dbutils.widgets.get("banco")


essential_columns = ['header__change_oper','_corrupt_record','header__timestamp','header__change_mask']
print(table_name)
print(keys)
print(owner)

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

sub_path = datalake_path['raw']+produto.lower()+"/"+banco.lower()+"/"
path = sub_path+""+owner+""+table_name#.upper()

print(path)


# COMMAND ----------

# MAGIC %run /general/functions/fnc_formata_nome_colunas

# COMMAND ----------

# MAGIC %run /general/functions/fnc_existencia_arquivo

# COMMAND ----------

#spark.conf.set("fs.azure.account.key."+datalake+".dfs.core.windows.net",dbutils.secrets.get(scope = "key-vault-"+env,key = "data-lake-key"))

# COMMAND ----------

if fnc_existencia_arquivo(path):
    print("Tabela "+owner+""+table_name+" listada para carga incremental")
else:
     dbutils.notebook.exit("tabela "+owner+""+table_name+" não listada em carga incremental")

# COMMAND ----------

table_list = spark.sql('show tables in ' + ods_database)
table_name_exist = table_list.filter(table_list.tableName == table_name.lower()).collect()

if len(table_name_exist)>0:
    print("Tabela encontrada em " + ods_database + ", começando a atualização...")
else:
    print("Criando "+table_name+" em " + ods_database)
    dbutils.notebook.run(
      "/management/ods_tables/routines/ods_create_routine_delta",
      timeout_seconds = 12000,
      arguments = {'table_name':table_name,"chaves": keys,'ods_database':ods_database, 'owner':owner,'produto':produto,'banco':banco})
    dbutils.notebook.exit("tabela criada")

# COMMAND ----------

#ods_df = spark.sql("select * from " + ods_database+"."+table_name)
ods_df = DeltaTable.forName(spark, ods_database+"."+table_name)
#desf

# COMMAND ----------


max_date = ods_df.toDF().agg({"data_entrada_silver_utc": "max"}).collect()[0][0]

# COMMAND ----------

max_date

# COMMAND ----------

diretorio_incremental = path # + "/"+data_referencia_partition+"/*"
print(diretorio_incremental)

# COMMAND ----------

dir_list = dbutils.fs.ls(path)

# COMMAND ----------

dir_list

# COMMAND ----------

incremental_df_with_changed_keys = (spark
    .read
    .option("mergeSchema", "true")
    .format("delta")
    #.option("primitivesAsString",'true')
    .load(diretorio_incremental))

incremental_df_with_changed_keys = incremental_df_with_changed_keys.withColumn('data_entrada_silver_utc',current_timestamp())

# COMMAND ----------



# COMMAND ----------

incremental_df_with_changed_keys = incremental_df_with_changed_keys.where(f'data_entrada_raw_utc >= "{max_date}"')

# COMMAND ----------

if incremental_df_with_changed_keys.rdd.isEmpty():
  dbutils.notebook.exit("Sem incremental")

# COMMAND ----------

#removendo registros que tiveram alteração em chave primária para evitar erros de homologação
before_img_df,incremental_df = remove_changed_keys(incremental_df_with_changed_keys,\
                                     keys,\
                                     datalake_path,\
                                    produto,\
                                    banco,\
                                    owner,\
                                    table_name)

# COMMAND ----------

ods_columns = spark.sql("SHOW COLUMNS FROM "+ods_database+"."+table_name)

# COMMAND ----------

display(ods_columns)

# COMMAND ----------

ods_columns_list = columns_full = ods_columns.select(ods_columns["col_name"]).rdd.flatMap(lambda x: x).collect()
type(ods_columns_list)

# COMMAND ----------

#Vendo quais colunas foram dropadas na origem para realizar um "soft drop" na delta, tendo em vista que a delta não permite drop de colunas
columns_to_drop = list(set(map(str.lower,ods_columns_list)) - set(map(str.lower,incremental_df.columns))) 

print(columns_to_drop)

# COMMAND ----------

incremental_df.columns

# COMMAND ----------

incremental_df = fnc_formata_nome_colunas(incremental_df)
incremental_df = incremental_df.select("*").where('header__change_oper in ("D","I","U")')
incremental_df.createOrReplaceTempView('temp_ods_table')
incremental_df = sqlContext.sql("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "+keys+" ORDER BY header__timestamp desc, header__change_seq desc) AS ROWNUM FROM temp_ods_table)  WHERE ROWNUM = 1")
incremental_df = incremental_df.drop('rownum')

# COMMAND ----------

#display(incremental_df)

# COMMAND ----------

for column_name in columns_to_drop:
  spark.sql('UPDATE '+ods_database+"."+table_name+' SET '+column_name+' = null')

# COMMAND ----------

keys_list = keys.split(",")
keys_list

# COMMAND ----------

ods_alias = 'a'
incremental_alias = 'b'

where_string = ''

for chave in keys_list:
  where_string = where_string + "{0}.{2} = {1}.{2} and ".format(ods_alias,incremental_alias,chave)

where_string_upd = where_string + " to_timestamp(ifnull({0}.header__timestamp,{0}.data_carga_utc)) < to_timestamp(ifnull({1}.header__timestamp,{1}.data_carga_utc)) ".format(ods_alias,incremental_alias)

where_string = where_string[:-5]

where_string

# COMMAND ----------

before_img_df = fnc_formata_nome_colunas(before_img_df)
before_img_df = before_img_df.select("*").where('header__change_oper in ("B")')
before_img_df.createOrReplaceTempView('temp_bef_table')
before_img_df = sqlContext.sql("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "+keys+" ORDER BY header__timestamp asc, header__change_seq asc) AS ROWNUM FROM temp_bef_table)  WHERE ROWNUM = 1")
before_img_df = before_img_df.drop('rownum')

# COMMAND ----------

#limpeza dos registros que sofreram mudança de chave

ods_df.alias(ods_alias).merge(
    before_img_df.alias(incremental_alias),
    where_string_upd)\
    .whenMatchedDelete() \
    .execute()

# COMMAND ----------

#atualizar apenas quando a change for mais recente
ods_df.alias(ods_alias).merge(
    incremental_df.alias(incremental_alias),
    where_string_upd)\
    .whenMatchedUpdateAll() \
    .execute()

# COMMAND ----------


ods_df.alias(ods_alias).merge(
    incremental_df.alias(incremental_alias),
    where_string)\
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

 spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False) #adicionado 

# COMMAND ----------

spark.sql(f"OPTIMIZE {ods_database}.{table_name}")
#spark.sql("VACUUM "+ods_database+"."+table_name)
spark.sql(f"VACUUM {ods_database}.{table_name} RETAIN 0 HOURS")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True) #adicionado

# COMMAND ----------


