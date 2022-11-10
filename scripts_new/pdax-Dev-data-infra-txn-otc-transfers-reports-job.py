"""
@copyright: PDAX 2022
@author: PDAX

notes: 
jobRunMode = 1 for full historical data run, 0 for daily extract

"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import input_file_name
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from dateutil import tz

def get_dates():
    G_TIMEZONE = "Asia/Manila"

    # Datetime is in UTC since DB stores data in UTC
    now_utc = datetime.now(timezone.utc)

    # Get applications from previous day (T-1 12AM PST until T 12AM PST, where T = current day)
    # Note: 12AM PST is 4PM UTC
    now = now_utc.astimezone(tz=tz.gettz(G_TIMEZONE))
    now = now.strptime(now.strftime('%Y-%m-%d 16:00:00.000'), '%Y-%m-%d %H:%M:%S.%f')

    end_date = now - timedelta(days=1)         # T-0 4PM UTC
    start_date = end_date - timedelta(days=1)  # T-1 4PM UTC

    start_date = start_date.replace(tzinfo=tz.gettz(G_TIMEZONE)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end_date = end_date.replace(tzinfo=tz.gettz(G_TIMEZONE)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    now_date = now_utc.astimezone(tz=tz.gettz(G_TIMEZONE))

    return start_date, end_date, now_date
    

# SETUP  
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'stage', 'env','jobRunMode','dateDelay'])
stage = args['stage']
env = args['env']
jobRunMode = args['jobRunMode']
dateDelay = args['dateDelay']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pdax-data-stage-trxn-pull-raw/otc/otc_transfers/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

S3bucket_node1.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("otc_transfers_raw")


query="""
SELECT
user_id,
transaction_id,
replace(trim(wallet_account),'"','') as wallet_account,
CASE 
  WHEN ops_con_uuid LIKE '%<nil>%' THEN null else ops_con_uuid
END as ops_con_uuid,
CASE 
  WHEN ops_request_id LIKE '%<nil>%' THEN null else ops_request_id
END as ops_request_id,
CASE 
  WHEN custody_txn_id LIKE '%<nil>%' THEN null else custody_txn_id
END as custody_txn_id,
CASE 
  WHEN sender_uuid LIKE '%<nil>%' THEN null else sender_uuid
END as sender_uuid,
CASE 
  WHEN receiver_uuid LIKE '%<nil>%' THEN null else receiver_uuid
END as receiver_uuid,
txn_type,
CASE 
  WHEN credit_ccy LIKE '%<nil>%' THEN null else credit_ccy
END as credit_ccy,
CASE 
  WHEN credit_amount LIKE '%<nil>%' THEN null else credit_amount
END as credit_amount,
CASE 
  WHEN credit_net_amount LIKE '%<nil>%' THEN null else credit_net_amount
END as credit_net_amount,
CASE 
  WHEN debit_ccy LIKE '%<nil>%' THEN null else debit_ccy
END as debit_ccy,
CASE 
  WHEN debit_amount LIKE '%<nil>%' THEN null else debit_amount
END as debit_amount,
CASE 
  WHEN debit_net_amount LIKE '%<nil>%' THEN null else debit_net_amount
END as debit_net_amount,
CASE 
  WHEN pdax_deposit_fee LIKE '%<nil>%' THEN null else pdax_deposit_fee
END as pdax_deposit_fee,
status,
created_date,
update_date,
cast(from_utc_timestamp(created_date, 'Asia/Manila') as timestamp) as created_date_pst,
substr(input_file_name, -28) as filename,
"""+jobRunMode+""" as jobRunMode
FROM otc_transfers_raw
"""

otc_transfers_raw = spark.sql(query).createOrReplaceTempView("otc_transfers_raw_TEMP")
start_date, end_date, now_date = get_dates()
query="""
SELECT * FROM (
SELECT 
*,
to_date(created_date_pst) as p_date,
substr(filename,1,10) AS filename_DATE,
date_sub(current_date, """+dateDelay+""") as date_delay,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp,
'{}' as startdate,
'{}' as enddate
FROM otc_transfers_raw_TEMP)a
WHERE (CASE WHEN jobRunMode = 1 THEN (date_sub(p_date,1) = filename_DATE)
      WHEN jobRunMode <> 1 THEN (date_sub(p_date,1) = filename_DATE) and (created_date >= startdate AND created_date <= enddate)
     END)
""".format(now_date, start_date, end_date)

otc_transfers_df = spark.sql(query)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
otc_transfers_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/mob_otc/otc_transfers/")

#data_dynamicframe = DynamicFrame.fromDF(otc_transfers_df.repartition(1), glueContext, "data_dynamicframe")


# Script generated for node Amazon Redshift
#AmazonRedshift_node4 = glueContext.write_dynamic_frame.from_catalog(
#    frame=data_dynamicframe,
#    database="spectrumdb_pdax_data_"+env+"",
#    table_name="spectrumdb_pdax_data_"+env+"_otc_transfers_"+env+"",
#    redshift_tmp_dir=args["TempDir"],
#    transformation_ctx="AmazonRedshift_node4",
#)


job.commit()