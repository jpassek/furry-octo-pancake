#Cloud Shell command for creating a BQ and Jupyter friendly Dataproc cluster

#$ gcloud beta dataproc clusters create spark-topicmodeling3 \
#--optional-components=ANACONDA,JUPYTER \
#--image-version=1.3 \
#--enable-component-gateway \
#--bucket spark_topicmodeling \
#--project sauron-230322 \
#--initialization-actions gs://sauron-dataproc-jupyter/connectors/connectors.sh \
#--metadata gcs-connector-version=2.0.0 \
#--metadata bigquery-connector-version=1.0.0 \
#--region=us-west1--

#Set up environment
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession
import json
import py4j
from pyspark import SparkConf,SparkContext
from __future__ import absolute_import
from pyspark.sql.functions import col, udf, array, collect_list, col
import pprint
import subprocess

# instantiate Spark
spark = SparkSession.builder.getOrCreate()

#When working outside of GCP
#sc_conf = SparkConf()
#sc_conf.setAppName("testApp") #// your spark application name
#sc_conf.setMaster('local[*]') #// specify master to run on local mode
#sc = pyspark.SparkContext(conf=sc_conf)
#sc = SparkContext.getOrCreate(conf=conf)
#sc = SparkContext.getOrCreate();

#bigquery conf
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input_bq'.format(bucket)

conf = {
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': '<projectid>',
    'mapred.bq.input.dataset.id': '<datasetid>',
    'mapred.bq.input.table.id': '<tableid>'}

#Loading Data from BQ table
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)

#Providing SQL context for a flat table with headers/converting
sql_context = SQLContext(sc)
wordT = table_data.map(lambda x : x[1])
tableDf=sql_context.read.json(wordT)

tableDf.show()

#Create dataframe for FPGrowth model input
labelsets = tableDf.groupBy("creative_id").agg(F.collect_set("labels"))

labelsets.show()

#Model Parameters
fpGrowth = FPGrowth(itemsCol="collect_set(labels)", minSupport=0.001, minConfidence=0.5)

#Model
model = fpGrowth.fit(labelsets)

#Dispaly Model Output
#Display frequent itemsets.
model.freqItemsets.show()
#Display generated association rules.
model.associationRules.show()
#transform examines the input items against all the association rules and summarize the
#consequents as prediction
model.transform(labelsets).show()

#Take a look at schema
model.associationRules.printSchema()

# Output Parameters.
output_dataset = 'oculi_30602'
output_table = 'fpgrowth_freq'

# Stage data formatted as newline-delimited JSON in Cloud Storage.
output_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_output1'.format(bucket)
output_files = output_directory + '/part-*'

#sql_context = SQLContext(sc)
#in this context, model output table is model.freqItemsets, model.associationRules or model.transform(labelsets)
(<model_output_table>
 .write.format('json').save(output_directory))

# Shell out to bq CLI to perform BigQuery import.
subprocess.check_call(
    'bq load --source_format NEWLINE_DELIMITED_JSON '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table, files=output_files
    ).split())

# Manually clean up the staging_directories, otherwise BigQuery
# files will remain indefinitely.
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)

#Delete temp bucket files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
