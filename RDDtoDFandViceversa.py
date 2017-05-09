
# coding: utf-8

# In[3]:

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
sc=SparkContext.getOrCreate()
sqlContext=SQLContext(sc)
l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]
rdd = sc.parallelize(l)
people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
schemaPeople = sqlContext.createDataFrame(people)


# In[4]:

type(schemaPeople)


# In[5]:

schemaPeople.show()


# In[10]:

rddFromDf=schemaPeople.rdd
rddFromDf.take(5)


# In[14]:

newRDD=rddFromDf.filter(lambda raw: "Ankit" not in raw.name)
newRDD.take(5)


# In[15]:

againDf=sqlContext.createDataFrame(newRDD)


# In[16]:

againDf.show()


# In[ ]:



