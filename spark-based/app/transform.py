"""PySpark reference ETL with persisted Delta tables and pinned batch snapshots."""
import json
from pyspark.sql import SparkSession,functions as F,Window
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from common import ROOT

def session():
    return configure_spark_with_delta_pip(SparkSession.builder.master('local[2]').appName('LateArrivalsETL')
      .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
      .config('spark.driver.memory','1g')).getOrCreate()

DATA=StructType([StructField('customer_id',StringType()),StructField('name',StringType()),StructField('segment',StringType()),StructField('effective_from',StringType()),StructField('event_time',StringType()),StructField('amount',DecimalType(18,2)),StructField('status',StringType())])
EVENT=StructType([StructField('event_id',LongType()),StructField('kind',StringType()),StructField('id',StringType()),StructField('revision',LongType()),StructField('op',StringType()),StructField('data',DATA),StructField('source_time',StringType())])

def write_delta(spark,df,name):
    path=str(ROOT/'delta'/name)
    df.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(path)
    return DeltaTable.forPath(spark,path).history(1).select('version').first()[0]

def transform(spark,batch):
    incoming=spark.read.schema(EVENT).json(str(ROOT/'json'/f'batch-{batch:06d}.jsonl')).withColumn('extracted_batch',F.lit(batch))
    raw=str(ROOT/'delta'/'events')
    if DeltaTable.isDeltaTable(spark,raw):
        DeltaTable.forPath(spark,raw).alias('t').merge(incoming.alias('s'),'t.event_id=s.event_id').whenNotMatchedInsertAll().execute()
    else:incoming.write.format('delta').save(raw)
    all_events=spark.read.format('delta').load(raw)
    window=Window.partitionBy('kind','id').orderBy(F.desc('revision'),F.desc('event_id'))
    state=all_events.withColumn('rn',F.row_number().over(window)).where('rn=1').drop('rn')
    customers=state.where("kind='customer' AND op!='d'").select(F.col('id').alias('cid'),F.col('data.name').alias('name'))
    history=state.where("kind='customer_history' AND op!='d'").select(F.col('id').alias('history_id'),F.col('data.customer_id').alias('customer_id'),F.col('data.segment').alias('segment'),F.to_timestamp('data.effective_from').alias('valid_from'),'revision')
    canonical=Window.partitionBy('customer_id','valid_from').orderBy(F.desc('revision'),F.desc('history_id'))
    history=history.withColumn('rn',F.row_number().over(canonical)).where('rn=1').drop('rn')
    history=history.withColumn('valid_to',F.coalesce(F.lead('valid_from').over(Window.partitionBy('customer_id').orderBy('valid_from')),F.to_timestamp(F.lit('2100-01-01'))))
    dim=history.join(customers,history.customer_id==customers.cid,'inner').select('customer_id',F.coalesce('name',F.lit('')).alias('name'),'segment','valid_from','valid_to')
    dim=dim.withColumn('customer_sk',F.sha2(F.concat_ws('|','customer_id',F.col('valid_from').cast('string')),256)).withColumn('inferred',F.lit(0))
    unknown=spark.createDataFrame([('unknown','unknown','Unknown customer','unknown','1900-01-01','2100-01-01',1)],'customer_sk string,customer_id string,name string,segment string,valid_from string,valid_to string,inferred int').withColumn('valid_from',F.to_timestamp('valid_from')).withColumn('valid_to',F.to_timestamp('valid_to'))
    dim=dim.select('customer_sk','customer_id','name','segment','valid_from','valid_to','inferred').unionByName(unknown)
    orders=state.where("kind='order'").select(F.col('id').alias('order_id'),F.col('data.customer_id').alias('customer_id'),F.col('data.amount').alias('amount'),F.to_timestamp('data.event_time').alias('event_time'),F.col('data.status').alias('status'),(F.col('op')=='d').cast('int').alias('deleted'))
    facts=orders.alias('o').join(customers.alias('c'),F.col('o.customer_id')==F.col('c.cid'),'left').join(dim.where('inferred=0').alias('d'),(F.col('o.customer_id')==F.col('d.customer_id'))&(F.col('o.event_time')>=F.col('d.valid_from'))&(F.col('o.event_time')<F.col('d.valid_to')),'left').select('o.*',F.coalesce(F.col('d.customer_sk'),F.lit('unknown')).alias('customer_sk'),F.col('c.cid').isNotNull().cast('int').alias('customer_known'),F.coalesce(F.col('d.segment'),F.lit('unknown')).alias('segment'))
    if facts.where('amount IS NULL OR event_time IS NULL OR customer_id IS NULL').limit(1).count():raise ValueError('Invalid order payload: typed parsing produced nulls')
    revenue=facts.where("deleted=0 AND status='paid'").groupBy(F.to_date('event_time').alias('day'),'segment').agg(F.sum('amount').alias('revenue'),F.count('*').alias('orders'))
    manifest={name:write_delta(spark,df,name) for name,df in [('dim_customer',dim),('fact_order',facts),('daily_revenue',revenue)]}
    manifest['events']=int(DeltaTable.forPath(spark,raw).history(1).select('version').first()[0])
    return {k:int(v) for k,v in manifest.items()}
