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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env','stage','jobRunMode','dateDelay'])
env = args['env']
stage = args['stage']
jobRunMode = args['jobRunMode']
dateDelay = args['dateDelay']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
otc_transfers_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "multiline": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pdax-data-"+env+"-trxn-pull-staging/mob_otc/otc_transfers/"],
        "recurse": True,
    },
    transformation_ctx="otc_transfers_node1",
)

# Script generated for node Amazon S3
osl_payments_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://anxone-prod-ap-southeast-1/paymentreports/"
        ],
        "recurse": True,
    },
    transformation_ctx="osl_payments_node2",
)
osl_payments_mapping_node4 = ApplyMapping.apply(
    frame=osl_payments_node2,
    mappings=[
        ("user_id", "string", "user_id", "string"),
        ("email", "string", "email", "string"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("reference_number", "string", "reference_number", "string"),
        ("method", "string", "method", "string"),
        ("channel", "string", "channel", "string"),
        ("amount", "string", "amount", "string"),
        ("fee", "string", "fee", "string"),
        ("status", "string", "status", "string"),
        ("deposit_code", "string", "deposit_code", "string"),
        ("referral_code", "string", "referral_code", "string"),
        ("txn_uuid", "string", "txn_uuid", "string"),
        ("update_date", "string", "update_date", "string"),
        ("create_date", "string", "create_date", "string"),
    ],
    transformation_ctx="osl_payments_mapping_node4",
)
osl_payments_mapping_node4.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("payments")
otc_transfers_node1.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("otc_transfers")

start_date, end_date, now_date = get_dates()
query = """
SELECT 
username,
transactiontype,
ccy,
amount,
funding_channel,
transactiondatetime,
channel,
producttype,
process_date_timestamp,
p_date
FROM(
SELECT 
to_date(update_date) as p_date,
lower(email) as username,
method as transactiontype,
'PHP' as ccy,
amount,
channel as funding_channel,
update_date as transactiondatetime,
'osl' as channel,
"""+jobRunMode+""" as jobRunMode,
date_sub(current_date,"""+dateDelay+""") as date_delay,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp,
'Cash In/Out' as producttype,
row_number()OVER (partition BY lower(email),reference_number ORDER BY update_date DESC) AS rown
    FROM payments
    WHERE status = 'success'
    AND lower(email) NOT LIKE '%osl.com'
    AND lower(email) NOT LIKE '%bc.holdings'
    )
WHERE rown = 1
AND (
CASE WHEN jobRunMode = 1 then (p_date < date_delay)
     WHEN jobRunMode <> 1 then (p_date = date_delay)
END)
""".format(now_date)
payments_df = spark.sql(query).createOrReplaceTempView("payments_final")

start_date, end_date, now_date = get_dates()
query = """
SELECT * FROM(
SELECT
wallet_account as username,
txn_type as transactiontype,
credit_ccy,
cast(credit_amount as decimal(19,6)) as credit_amount,
debit_ccy,
cast(debit_amount as decimal(19,6)) as debit_amount,
update_date,
from_utc_timestamp(update_date, 'Asia/Manila') as transactiondatetime,
from_utc_timestamp(created_date, 'Asia/Manila') as createddatetime,
filename_DATE as otc_filename,
to_date(from_utc_timestamp(update_date, 'Asia/Manila')) as p_date,
date_sub(current_date,"""+dateDelay+""") as date_delay,
"""+jobRunMode+""" as jobRunMode,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp,
'{}' as startdate,
'{}' as enddate
from 
otc_transfers
where txn_type in ('deposit','withdrawal')
and lower(status) = 'completed')a
WHERE
(CASE WHEN jobRunMode = 1 then (p_date < date_delay)
     WHEN jobRunMode <> 1 then (p_date = date_delay)
END)
""".format(now_date, start_date, end_date)
transfers_df = spark.sql(query).createOrReplaceTempView("transfers_df")

query="""
SELECT
username,
case when transactiontype = 'deposit' then 'cashIn' end as transactiontype,
credit_ccy as ccy,
credit_amount as amount,
'mob_otc' as funding_channel,
transactiondatetime,
'mob_otc' as channel,
'Cash In/Out' as producttype,
process_date_timestamp,
p_date
FROM transfers_df
where transactiontype = 'deposit'
"""
cashin_df = spark.sql(query).createOrReplaceTempView("cashin")

query="""
SELECT
username,
case when transactiontype = 'withdrawal' then 'cashOut' end as transactiontype,
debit_ccy as ccy,
debit_amount as amount,
'mob_otc' as funding_channel,
transactiondatetime,
'mob_otc' as channel,
'Cash In/Out' as producttype,
process_date_timestamp,
p_date
FROM transfers_df
where transactiontype = 'withdrawal'
"""
cashout_df = spark.sql(query).createOrReplaceTempView("cashout")

query="""
SELECT
username,
transactiontype,
ccy,
case when amount = null or amount = '' or length(amount) = 0  then 0 
    else cast(amount as decimal(19,6)) end as amount,
funding_channel,
cast(transactiondatetime as timestamp) as transactiondatetime,
channel,
producttype,
process_date_timestamp,
p_date
FROM (SELECT * FROM cashin
UNION ALL
SELECT * FROM cashout
UNION ALL
SELECT * FROM payments_final)

"""

final_df = spark.sql(query)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
final_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/master_transaction/cashin_cashout/")


data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")

AmazonRedshift_node5 = glueContext.write_dynamic_frame.from_catalog(
    frame=data_dynamicframe,
    database="pdax_data_infra_reports",
    table_name="spectrumdb_pdax_data_"+env+"_master_transaction_cashin_cashout",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node5",
)

job.commit()