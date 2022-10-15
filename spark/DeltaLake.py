#!/usr/bin/env python
# coding: utf-8

# ## DeltaLake
# 
# 
# 

# In[3]:


get_ipython().run_cell_magic('pyspark', '', '\r\n# https://docs.delta.io/latest/quick-start.html\r\n\r\n# Create Table\r\ndata = spark.range(0, 5)\r\ndata.write.format("delta").save("abfss://delta@oszamora.dfs.core.windows.net/delta-table")\r\n')


# In[4]:


#  Read  Data

df = spark.read.format("delta").load("abfss://delta@oszamora.dfs.core.windows.net/delta-table")
df.show()


# In[5]:


# Update Table

data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save("abfss://delta@oszamora.dfs.core.windows.net/delta-table")


# In[6]:


# Conditional Update

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "abfss://delta@oszamora.dfs.core.windows.net/delta-table")

# Update every even value by adding 100 to it
deltaTable.update(
  condition = expr("id % 2 == 0"),
  set = { "id": expr("id + 100") })

# Delete every even value
deltaTable.delete(condition = expr("id % 2 == 0"))

# Upsert (merge) new data
newData = spark.range(0, 20)

deltaTable.alias("oldData") \
  .merge(
    newData.alias("newData"),
    "oldData.id = newData.id") \
  .whenMatchedUpdate(set = { "id": col("newData.id") }) \
  .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
  .execute()

deltaTable.toDF().show()


# In[8]:


# Time Travel

df = spark.read.format("delta").option("versionAsOf", 0).load("abfss://delta@oszamora.dfs.core.windows.net/delta-table")
df.show()

df = spark.read.format("delta").option("versionAsOf", 1).load("abfss://delta@oszamora.dfs.core.windows.net/delta-table")
df.show()


# In[9]:


# Write a stream of data to a table

streamingDf = spark.readStream.format("rate").load()
stream = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "abfss://delta@oszamora.dfs.core.windows.net/checkpoint").start("abfss://delta@oszamora.dfs.core.windows.net/delta-table")


# In[13]:


# Read a stream of changes from a table

stream2 = spark.readStream.format("delta").load("abfss://delta@oszamora.dfs.core.windows.net/delta-table").writeStream.format("console").start()

