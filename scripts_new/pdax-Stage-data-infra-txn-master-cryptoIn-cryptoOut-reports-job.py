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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env','stage','dateDelay','jobRunMode'])
env = args['env']
stage = args['stage']
dateDelay = args['dateDelay']
jobRunMode = args['jobRunMode']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
#pDate_filter2 = "partition_0 == year(date_sub(current_date,"+dateDelay+")) AND partition_1 == month(date_sub(current_date,"+dateDelay+")) AND partition_2 == day(date_sub(current_date,"+dateDelay+"))"

otc_transfers_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pdax-data-"+env+"-trxn-pull-staging/mob_otc/otc_transfers/"],
        "recurse": True,
    },
    transformation_ctx="otc_transfers_node1",
)

osl_transactions_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="pdax_data_infra_reports",
    table_name="osl_transactions",
#   push_down_predicate= pDate_filter2,
    transformation_ctx="osl_transactions_node2",
)


osl_transactions_mapping_node3  = ApplyMapping.apply(
    frame=osl_transactions_node2,
    mappings=[
        ("ccy", "string", "ccy", "string"),
        ("username", "string", "username", "string"),
        ("transactionstate", "string", "transactionstate", "string"),
        ("transactiontype", "string", "transactiontype", "string"),
        ("amount", "string", "amount", "string"),
        ("processeddatetime", "string", "processeddatetime", "string"),
        ("partition_0", "string", "pxn_yr", "string"),
        ("partition_1", "string", "pxn_mo", "string"),
        ("partition_2", "string", "pxn_dy", "string"),
    ],
    transformation_ctx="osl_transactions_mapping_node3",
)

otc_transfers_node1.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("otc_transfers")
osl_transactions_mapping_node3.toDF().createOrReplaceTempView("osl_transactions")

start_date, end_date, now_date = get_dates()
query="""
SELECT 
username,
transactiontype,
ccy,
amount,
transactiondatetime,
channel,
producttype,
process_date_timestamp,
p_date
FROM (
select
username,
CASE when transactiontype = 'DEPOSIT' then 'cryptoIn'
     when transactiontype = 'WITHDRAWAL' then 'cryptoOut' end
 as transactiontype,
ccy,
case when (amount is null or amount = '' or length(amount) = 0) then 0
else cast(amount as decimal(19,6)) end as amount,
cast(processeddatetime as timestamp) as transactiondatetime,
'osl' as channel,
'Crypto In/Out' as producttype,
concat(pxn_yr,'-',pxn_mo,'-',pxn_dy) as p_date,
"""+jobRunMode+""" as jobRunMode,
date_sub(current_date,"""+dateDelay+""") as date_delay,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp
from osl_transactions
where transactiontype in ('DEPOSIT','WITHDRAWAL')
AND ccy != 'PHP'
AND transactionstate = 'PROCESSED'
AND username NOT LIKE '%osl.com'
AND username NOT LIKE '%bc.holdings')a
WHERE 
(
CASE WHEN jobRunMode = 1 then (p_date < date_delay)
     WHEN jobRunMode <> 1 then (p_date = date_delay)
END)

""".format(now_date)
transactions_df = spark.sql(query) 

start_date, end_date, now_date = get_dates()
query = """
SELECT *
FROM (
SELECT
wallet_account as username,
txn_type as transactiontype,
credit_ccy,
cast(credit_net_amount as decimal(19,6)) as credit_amount,
debit_ccy,
cast(debit_net_amount as decimal(19,6)) as debit_amount,
update_date,
cast(from_utc_timestamp(update_date, 'Asia/Manila') as timestamp) as transactiondatetime,
'mob_otc' as channel,
'Crypto In/Out' as producttype,
filename_DATE as otc_filename,
date_sub(current_date,"""+dateDelay+""") as date_delay,
"""+jobRunMode+""" as jobRunMode,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp,
to_date(from_utc_timestamp(update_date, 'Asia/Manila')) as p_date
from 
otc_transfers
where txn_type in ('crypto_in','crypto_out')
and lower(status) = 'completed')a
WHERE(CASE WHEN jobRunMode = 1 then (p_date < date_delay)
     WHEN jobRunMode <> 1 then (p_date = date_delay)
END)
""".format(now_date, start_date, end_date)
transfers_df = spark.sql(query).createOrReplaceTempView("transfers_df")

query="""
SELECT
username,
case when transactiontype = 'crypto_in' then 'cryptoIn' end as transactiontype,
credit_ccy as ccy,
credit_amount as amount,
transactiondatetime,
channel,
producttype,
process_date_timestamp,
p_date
FROM transfers_df
where transactiontype = 'crypto_in'
"""
crypto_in = spark.sql(query)

query="""

SELECT
username,
case when transactiontype = 'crypto_out' then 'cryptoOut' end as transactiontype,
debit_ccy as ccy,
debit_amount as amount,
transactiondatetime,
channel,
producttype,
process_date_timestamp,
p_date
FROM transfers_df
where transactiontype = 'crypto_out'
"""
crypto_out = spark.sql(query)

otc_transfers_final = crypto_in.union(crypto_out)
union_df = otc_transfers_final.union(transactions_df).createOrReplaceTempView("union_df")

query="""
SELECT
username,
transactiontype,
CASE 
   WHEN ccy = 'BNB_BSC' then 'BNB'
   WHEN ccy = 'AXS_RON' then 'AXS'
   WHEN ccy in ('BUSD_BSC','BUSD') then 'BUSD'
   WHEN ccy = 'MATIC_POLYGON' then 'MATIC'
   WHEN ccy = 'SLP_RON' then 'SLP'
   WHEN ccy = 'TERRA_USD' then 'UST'
   WHEN ccy = 'WETHMATIC' then 'ETH'
   WHEN ccy = 'WETH_POLYGON' then 'ETH'
   WHEN ccy = 'XLM_USDC_5F3T' then 'USDT'
   WHEN ccy like '%USD%' then 'USDT'
else ccy end as ccy,
amount,
transactiondatetime,
channel,
producttype,
process_date_timestamp,
p_date
FROM union_df
"""
final_df = spark.sql(query)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
final_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/master_transaction/cryptoin_cryptoout/")


data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")

AmazonRedshift_node5 = glueContext.write_dynamic_frame.from_catalog(
    frame=data_dynamicframe,
    database="pdax_data_infra_reports",
    table_name="spectrumdb_pdax_data_"+env+"_master_transaction_cryptoin_cryptoout",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node5",
)    


job.commit()