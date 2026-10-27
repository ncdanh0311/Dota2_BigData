"""Resolve and verify the configured JVM Kafka source without contacting a broker."""
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('DotaKafkaConnectorCheck').getOrCreate()
try:
    spark._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass('org.apache.spark.sql.kafka010.KafkaSourceProvider')
    print('Kafka connector resolved and loaded.')
finally:
    spark.stop()
