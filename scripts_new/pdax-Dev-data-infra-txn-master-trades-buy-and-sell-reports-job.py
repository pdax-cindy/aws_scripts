
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
dateDelay = args['dateDelay']
stage = args['stage']
jobRunMode = args['jobRunMode']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
#pDate_filter2 = "partition_0 == year(date_sub(current_date,"+dateDelay+")) AND partition_1 == month(date_sub(current_date,"+dateDelay+")) AND partition_2 == day(date_sub(current_date,"+dateDelay+"))"

# Script generated for node S3 bucket
otc_trades_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "multiline": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pdax-data-"+env+"-trxn-pull-staging/mob_otc/otc_trades/"],
        "recurse": True,
    },
    transformation_ctx="otc_trades_node1",
)

osl_transactions_node2 = glueContext.create_dynamic_frame.from_catalog(
    database="pdax_data_infra_reports",
    table_name="osl_transactions",
#    push_down_predicate= pDate_filter2,
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
        ("fee", "string", "fee", "string"),
        ("processeddatetime", "string", "processeddatetime", "string"),
        ("tradeid", "long", "tradeid", "long"),
        ("partition_0", "string", "pxn_yr", "string"),
        ("partition_1", "string", "pxn_mo", "string"),
        ("partition_2", "string", "pxn_dy", "string"),
    ],
    transformation_ctx="osl_transactions_mapping_node3",
)

osl_transactions_mapping_node3.toDF().createOrReplaceTempView("osl_transactions")
otc_trades_node1.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("otc_trades")

start_date, end_date, now_date = get_dates()
query = """
SELECT 
tradeid,
username,
transactiondatetime,
credit_ccy,
debit_ccy,
tradeccy_amount,
php_amount,
transactiontype,
fee,
channel,
producttype,
tradedccy,
process_date_timestamp,
p_date
FROM(
select 
transaction_id as tradeid,
wallet_account as username,
update_date,
cast(from_utc_timestamp(update_date, 'Asia/Manila')as timestamp) as transactiondatetime,
credit_ccy,
debit_ccy,
Case 
    when debit_ccy = 'PHPT'  then cast(debit_amt as decimal(19,6))
    when debit_ccy != 'PHPT' then cast(credit_amt as decimal(19,6))
end as php_amount,
Case
    when debit_ccy != 'PHPT'  then cast(debit_amt as decimal(19,6))
    when debit_ccy = 'PHPT' then cast(credit_amt as decimal(19,6))
end as tradeccy_amount,
trade_side as transactiontype,
0 as fee,
'mob_otc' as channel,
'Trade Buy/Sell' as producttype,
tradedcurrency as tradedccy,
"""+jobRunMode+""" as jobRunMode,
date_sub(current_date,"""+dateDelay+""") as date_delay,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp,
to_date(from_utc_timestamp(update_date, 'Asia/Manila')) as p_date
from otc_trades
where
 wallet_account NOT LIKE '%osl.com'
AND wallet_account NOT LIKE '%bc.holdings'
AND wallet_account NOT LIKE '%ftx.mobile.otc@pdax.ph'
and lower(status) = 'successful')a
WHERE (CASE WHEN jobRunMode = 1 then (p_date < date_delay)
     WHEN jobRunMode <> 1 then (p_date = date_delay)
END)
""".format(now_date, start_date, end_date)

otc_trades_df = spark.sql(query)

query="""
SELECT 
tradeid,
username,
transactiondatetime,
ccy,
amount,
transactiontype,
fee,
channel,
producttype,
p_date
FROM(
select
cast(tradeid as int) as tradeid,
username,
cast(processeddatetime as timestamp) as transactiondatetime,
ccy,
cast(amount as decimal(19,6)) as amount,
CASE WHEN ccy in ('PHP','PHPT') AND transactiontype = 'FILL_CREDIT' then 'buy'
     WHEN ccy in ('PHP','PHPT') AND transactiontype = 'FILL_DEBIT' then 'sell'
end as transactiontype,
fee,
'osl' as channel,
'Trade Buy/Sell' as producttype,
concat(pxn_yr,'-',pxn_mo,'-',pxn_dy) as p_date,
"""+jobRunMode+""" as jobRunMode,
date_sub(current_date,"""+dateDelay+""") as date_delay
from osl_transactions
where transactiontype in  ('FILL_CREDIT','FILL_DEBIT')
AND transactionstate = 'PROCESSED'
AND username NOT LIKE '%osl.com'
AND username NOT LIKE '%bc.holdings'
ORDER BY tradeid,transactiontype)a
WHERE
(
CASE WHEN jobRunMode = 1 then (p_date < date_delay)
     WHEN jobRunMode <> 1 then (p_date = date_delay)
END)
"""

transactions_df = spark.sql(query).createOrReplaceTempView("transactions_df")

query="""
SELECT
tradeid,
username,
transactiondatetime,
ccy as credit_ccy,
amount as php_amount,
fee,
transactiontype,
channel,
producttype,
p_date
from transactions_df
WHERE ccy in ('PHP','PHPT')
"""
osl_credit = spark.sql(query).createOrReplaceTempView("osl_credit") 


query="""
SELECT
tradeid,
username,
transactiondatetime,
ccy as debit_ccy,
amount as tradeccy_amount,
fee,
transactiontype,
channel,
producttype,
p_date
from transactions_df
WHERE ccy not in ('PHP','PHPT')"""
osl_debit = spark.sql(query).createOrReplaceTempView("osl_debit") 


start_date, end_date, now_date = get_dates()
query="""
SELECT *,
to_date(transactiondatetime) as p_date
FROM(
SELECT
a.tradeid,
a.username,
a.transactiondatetime,
a.credit_ccy,
b.debit_ccy,
b.tradeccy_amount,
a.php_amount,
a.transactiontype,
a.fee,
a.channel,
a.producttype,
'' as tradedccy,
from_utc_timestamp('{}', 'Asia/Manila') as process_date_timestamp
FROM
osl_credit a
LEFT JOIN
osl_debit b
ON a.tradeid = b.tradeid
AND a.p_date = b.p_date)a
""".format(now_date)
osl_trades_df = spark.sql(query)

final_df = osl_trades_df.union(otc_trades_df)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
final_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/master_transaction/trades_buy_and_sell/")

data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")

AmazonRedshift_node5 = glueContext.write_dynamic_frame.from_catalog(
    frame=data_dynamicframe,
    database="spectrumdb_pdax_data_dev",
    table_name="spectrumdb_pdax_data_"+env+"_master_transaction_trades_buy_and_sell",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node5",
)

#pdax_data_infra_reports
job.commit()