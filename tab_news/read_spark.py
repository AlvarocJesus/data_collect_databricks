# %%
from pyspark.sql import SparkSession

spark = (SparkSession
  .builder
  .appName('Python Spark SQL basic example')
  .config('spark.some.config.option', 'some-value')
  .getOrCreate())
# %%
df = spark.read.format('json').load('./data/contents/json/')

df.show()