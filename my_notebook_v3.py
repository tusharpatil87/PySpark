# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.withColumn('row_number', row_number().over(Window.orderBy('Item_Identifier')) ).display()

# COMMAND ----------

df.withColumn('row_number', row_number().over(Window.orderBy('Item_Identifier')) )\
    .withColumn('rank', rank().over(Window.orderBy('Item_Identifier')))\
        .withColumn('dens_rank', dense_rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('cum_sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow) )  )\
    .select(['Item_Type','Item_MRP','cum_sum' ]).display()

# COMMAND ----------

def temp_func(x):
    return x * x

# COMMAND ----------

my_udf = udf(temp_func)

# COMMAND ----------

df.withColumn('my_new_func', my_udf('Item_MRP')).display()

# COMMAND ----------

df_my_func = df.withColumn('my_new_func', my_udf('Item_MRP'))

# COMMAND ----------

df_my_func.limit(5).display()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df_my_func.write.format('csv')\
    .mode('append')\
        .option('header', True)\
        .save('/FileStore/tables/my_func_data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df_my_func.write.format('csv')\
    .mode('append')\
        .option('header', True)\
        .save('/FileStore/tables/my_func_data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

new_df = spark.read.format('csv').option('inferschema', True).option('header', True).load('dbfs:/FileStore/tables/my_func_data.csv/')

new_df.display()

# COMMAND ----------

df_my_func.display()

# COMMAND ----------

df_my_func.write.format('csv')\
    .mode('overwrite')\
        .option('header', True)\
        .save('/FileStore/tables/my_func_data.csv')

# COMMAND ----------

new_df = spark.read.format('csv').option('inferschema', True).option('header', True).load('dbfs:/FileStore/tables/my_func_data.csv/')

new_df.display()

# COMMAND ----------

new_df.write.format('csv').mode('error').option('header', True).save('/FileStore/tables/my_func_data.csv')

# COMMAND ----------

new_df.write.format('csv')\
        .mode('ignore').option('header', True)\
            .save('/FileStore/tables/my_func_data.csv')

# COMMAND ----------

new_df.write.format('parquet')\
        .mode('overwrite')\
            .save('/FileStore/tables/my_func_data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

new_df = spark.read.format('parquet').option('inferschema', True).option('header', True).load('dbfs:/FileStore/tables/my_func_data.csv/')

new_df.display()

# COMMAND ----------

new_df_1 = spark.read.format('csv').option('inferschema', True).option('header', True).load('dbfs:/FileStore/tables/my_func_data.csv/')

new_df_1.display()

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
        .saveAsTable('my_table_v1')

# COMMAND ----------

df.createTempView('my_tmp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM my_tmp_view WHERE Item_Fat_Content = 'Regular'

# COMMAND ----------

df_sql = spark.sql("SELECT * FROM my_tmp_view WHERE Item_Fat_Content = 'Regular' ")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

