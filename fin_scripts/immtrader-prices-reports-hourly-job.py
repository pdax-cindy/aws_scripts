import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name

from awsglue.dynamicframe import DynamicFrame

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
datasource0 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://anxone-prod-ap-southeast-1/immtrader/prices/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

dataframe1 = datasource0.toDF().withColumn("input_file_name", input_file_name()).createOrReplaceTempView("prices_raw")
query = """
with temp1 as (
SELECT *,
substr(filename,15,10) as p_date,
substr(filename,15,4) as pxn_yr,
substr(filename,20,2) as pxn_mo,
substr(filename,23,2) as pxn_dy,
substr(filename,26,2) as pxn_hr,
substr(filename,28,2) as pxn_mm,
substr(filename,30,2) as pxn_ss
FROM (
select *,
"""+jobRunMode+""" as jobRunMode,
substr(input_file_name, -35) as filename,
CAST(CURRENT_TIMESTAMP() as timestamp) as curr_timestamp
from prices_raw)a
)

SELECT *,
      update_date_pst - INTERVAL 8 HOUR AS update_date_utc,
      to_date(processdatetimestamp) as processdate
FROM (
        SELECT *,
            cast ( concat(p_date,' ',pxn_hr,':',pxn_mm,':',pxn_ss) as timestamp) as update_date_pst,
            curr_timestamp + INTERVAL 8 HOUR AS processdatetimestamp
        FROM temp1)
        WHERE
        (CASE WHEN jobRunMode = 1 THEN (p_date < to_date(processdatetimestamp))
             WHEN jobRunMode <> 1 THEN p_date = to_date(processdatetimestamp)
        END)
    
"""
prices_df1 = spark.sql(query).createOrReplaceTempView("prices_temp")

query= """
select
cryptopair,
tradedcurrency,
settlementcurrency,
TRIM(`market maker`) as market_maker,
TRIM(`imm best price-buy`) as imm_best_price_buy,
TRIM(`imm best price-sell`) as imm_best_price_sell,
TRIM(`coins.ph (fx)-buy`) as coins_ph_fx_buy,
TRIM(`coins.ph (fx)-sell`) as coins_ph_fx_sell,
TRIM(`coins.ph-buy`) as coins_ph_buy,
TRIM(`coins.ph-sell`) as coins_ph_sell,
TRIM(`bloomx-buy`) as bloomx_buy,
TRIM(`bloomx-sell`) as bloomx_sell,
TRIM(`pdax (fx)-buy`) as pdax_fx_buy,
TRIM(`pdax (fx)-sell`) as pdax_fx_sell,
TRIM(`cumberland-buy`) as cumberland_buy,
TRIM(`cumberland-sell`) as cumberland_sell,
TRIM(`ftx-buy`) as ftx_buy,
TRIM(`ftx-sell`) as ftx_sell,
TRIM(`whalefin-buy`) as whalefin_buy,
TRIM(`whalefin-sell`) as whalefin_sell,
TRIM(`falconx-buy`) as falconx_buy,
TRIM(`falconx-sell`) as falconx_sell,
TRIM(`b2c2-buy`) as b2c2_buy,
TRIM(`b2c2-sell`) as b2c2_sell,
TRIM(`wintermute-buy`) as wintermute_buy,
TRIM(`wintermute-sell`) as wintermute_sell,
TRIM(`best price-buy`) as best_price_buy,
TRIM(`best price-sell`) as best_price_sell,
filename,
update_date_pst,
update_date_utc,
p_date,
pxn_yr,
pxn_mo,
pxn_dy,
pxn_hr,
processdatetimestamp
from prices_temp
"""
final_df = spark.sql(query)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
final_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/immtrader_hourly/mm_prices/")

data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")

pre_query = "drop table if exists pdax_data_"+env+".mm_prices_stg;create table pdax_data_"+env+".mm_prices_stg as select * from pdax_data_"+env+".mm_prices where 1=2;"
post_query = "begin;delete from pdax_data_"+env+".mm_prices_stg using pdax_data_"+env+".mm_prices where pdax_data_"+env+".mm_prices.filename = pdax_data_"+env+".mm_prices_stg.filename; insert into pdax_data_"+env+".mm_prices select * from pdax_data_"+env+".mm_prices_stg; drop table pdax_data_"+env+".mm_prices_stg; end;"
             
AmazonRedshift_node4 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=data_dynamicframe,
    catalog_connection="glue-to-redshift",
    connection_options={
        "database": "spectrumdb",
        "dbtable": "pdax_data_"+env+".mm_prices_stg",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node4",
)
#data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")

# Script generated for node Amazon Redshift
#AmazonRedshift_node4 = glueContext.write_dynamic_frame.from_catalog(
#    frame=data_dynamicframe,
#    database="pdax_data_infra_reports",
#    table_name="spectrumdb_pdax_data_"+env+"_mm_prices",
#    redshift_tmp_dir=args["TempDir"],
#    transformation_ctx="AmazonRedshift_node4",
#)


job.commit()




