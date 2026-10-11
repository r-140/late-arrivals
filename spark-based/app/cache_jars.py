from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
s=configure_spark_with_delta_pip(SparkSession.builder.master('local[1]').appName('cache-delta')
 .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
 .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')).getOrCreate()
s.stop()
