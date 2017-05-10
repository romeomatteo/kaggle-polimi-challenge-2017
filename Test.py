from pyspark import *
from pyspark import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
sc = SparkContext.getOrCreate()

training_set_RDD = sc.textFile("train.csv")
training_set_RDD.take(5)
training_set_RDD = training_set_RDD.map(lambda line: line.split(","))
training_set_RDD = training_set_RDD.filter(lambda line: 'userId' not in line)
training_set_RDD = training_set_RDD.map(lambda line: (int(line[0]), int(line[1]), int(line[2])))
training_set_RDD.take(5)
sqlContext = SQLContext(sc)
training_set = sqlContext.createDataFrame(training_set_RDD, ["userId", "itemId", "rating"])

training_set.filter(training_set.itemId == 3506).show()

global_bias_df = training_set.select(mean("rating").alias("global_bias_attr"))

global_bias = global_bias_df.collect()[0].global_bias_attr
global_bias

training_set_r2 = training_set.withColumn('rating', training_set.rating - global_bias)

training_set_r2.filter(training_set_r2.itemId == 3506).show()
item_bias = training_set_r2.groupBy(training_set_r2.itemId).agg(mean(training_set_r2.rating).alias("bias"))
item_bias.show()
training_set_r3 = training_set_r2.withColumn('rating', training_set_r2.rating - item_bias.filter(training_set_r2.itemId == item_bias.itemId).collect()[0].bias)

training_set_r3.filter(training_set_r3.itemId == 3506).show()
