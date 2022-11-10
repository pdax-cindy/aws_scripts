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
    
"""
############ START OF SCRIPT ############
"""

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
        "paths": ["s3://pdax-data-stage-trxn-pull-raw/otc/otc_trades/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)


S3bucket_node1.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("otc_trades_raw")

query="""
SELECT
user_id,
transaction_id,
trim(wallet_account) as wallet_account,
debit_ccy,
case when (debit_amt = null or debit_amt = '' or length(debit_amt) = 0 or debit_amt LIKE '%<nil>%') then 0 
    else cast(debit_amt as decimal(19,6)) end as debit_amt,
case when (net_debit_amt = null or net_debit_amt = '' or length(net_debit_amt) = 0 or net_debit_amt LIKE '%<nil>%') then 0 
    else cast(net_debit_amt as decimal(19,6)) end as net_debit_amt,
credit_ccy,
case when (credit_amt = null or credit_amt = '' or length(credit_amt) = 0 or credit_amt LIKE '%<nil>%') then 0 
    else cast(credit_amt as decimal(19,6)) end as credit_amt,
case when (net_credit_amt = null or net_credit_amt = '' or length(net_credit_amt) = 0 or net_credit_amt LIKE '%<nil>%') then 0 
    else cast(net_credit_amt as decimal(19,6)) end as net_credit_amt,
tradedCurrency,
settlementCurrency,
mm_trade_id,
mm_service,
trade_side,
trade_type,
base_ccy,
trade_pair,
CASE 
  WHEN trading_fee LIKE '%{}%' THEN null else trading_fee
END as trading_fee,
CASE 
  WHEN spread_amt LIKE '%<nil>%' THEN null else spread_amt
END as spread_amt,
CASE 
  WHEN spread_rate LIKE '%<nil>%' THEN null else spread_rate
END as spread_rate,
CASE 
  WHEN mm_price LIKE '%<nil>%' THEN null else mm_price
END as mm_price,
CASE 
  WHEN fx_rate LIKE '%<nil>%' THEN null else fx_rate
END as fx_rate,
execution_price,
settlement_date,
CASE 
  WHEN filled_order_traded_unit_value LIKE '%<nil>%' THEN null else filled_order_traded_unit_value
END as filled_order_traded_unit_value,
CASE 
  WHEN filled_order_traded_unit_currency LIKE '%<nil>%' THEN null else filled_order_traded_unit_currency
END as filled_order_traded_unit_currency,
CASE 
  WHEN filled_order_unit_price_value LIKE '%<nil>%' THEN null else filled_order_unit_price_value
END as filled_order_unit_price_value,
CASE 
  WHEN filled_order_unit_price_currency LIKE '%<nil>%' THEN null else filled_order_unit_price_currency
END as filled_order_unit_price_currency,
CASE 
  WHEN filled_order_total_amount_value LIKE '%<nil>%' THEN null else filled_order_total_amount_value
END as filled_order_total_amount_value,
CASE 
  WHEN filled_order_total_amount_currency LIKE '%<nil>%' THEN null else filled_order_total_amount_currency
END as filled_order_total_amount_currency,
status,
create_date,
update_date,
cast(from_utc_timestamp(update_date, 'Asia/Manila') as timestamp) as update_date_pst,
cast(from_utc_timestamp(create_date, 'Asia/Manila') as timestamp) as create_date_pst,
substr(input_file_name, -25) as filename,
"""+jobRunMode+""" as jobRunMode
FROM otc_trades_raw
"""

otc_trades_raw = spark.sql(query).createOrReplaceTempView("otc_trades_raw_TEMP")
start_date, end_date, now_date = get_dates()
query="""
SELECT *
FROM (
SELECT
*,
to_date(create_date_pst) as p_date,
to_date(substr(filename,1,10)) AS filename_DATE,
date_sub(current_date, """+dateDelay+""") as date_delay,
from_utc_timestamp('{}', 'Asia/Manila') as processdate_timestamp
FROM otc_trades_raw_TEMP)a
WHERE
(CASE WHEN jobRunMode = 1 THEN (date_sub(p_date,1) = filename_DATE)
      WHEN jobRunMode <> 1 THEN (date_sub(p_date,1)= filename_DATE
                                    AND (create_date >= '{}' AND create_date <= '{}'))
      END)
""".format(now_date,start_date,end_date)
otc_trades_final = spark.sql(query)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
otc_trades_final.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/mob_otc/otc_trades/")


job.commit()

