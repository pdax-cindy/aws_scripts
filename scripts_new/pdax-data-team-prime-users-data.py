import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env','dateDelay'])
env = args['env']
dateDelay = args['dateDelay']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
pDate_filter = "partition_0 == year(date_sub(current_date, "+dateDelay+")) AND partition_1 == month(date_sub(current_date, "+dateDelay+")) AND partition_2 == day(date_sub(current_date, "+dateDelay+"))"


S3bucket_country_node0 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://pdax-data-dev-trxn-pull-staging/manual_files/country_list/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_country_node0",
)

# Script generated for node S3 bucket
S3bucket_prime_user_node0 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://pdax-data-dev-trxn-pull-raw/manual_files/prime_users_tagging/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_prime_user_node0",
)

S3bucket_premium_user_node0 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://pdax-data-dev-trxn-pull-raw/manual_files/pdax_premium_users_upgrade/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_premium_user_node0",
)

S3bucket_prime_user_historical_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://pdax-data-dev-trxn-pull-raw/manual_files/pdax_prime_users_historical/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_prime_user_historical_node1",
)

S3bucket_premium_user_historical_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://pdax-data-dev-trxn-pull-raw/manual_files/pdax_premium_users_historical/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_premium_user_historical_node1",
)

S3bucket_prime_user_upgrade_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://pdax-data-dev-trxn-pull-raw/manual_files/pdax_prime_users_upgrade/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_prime_user_upgrade_node2",
)

S3bucket_prime_corporate_node3 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://pdax-data-dev-trxn-pull-raw/manual_files/pdax_prime_corporate_clients/"
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_prime_corporate_node3",
)

S3bucket_prime_user_node0.toDF().createOrReplaceTempView("prime_tagging_users")
S3bucket_prime_user_upgrade_node2.toDF().createOrReplaceTempView("prime_users_upgrade")
S3bucket_prime_user_historical_node1.toDF().createOrReplaceTempView("prime_users_historical")
S3bucket_premium_user_node0.toDF().createOrReplaceTempView("premium_users_upgrade")
S3bucket_premium_user_historical_node1.toDF().createOrReplaceTempView("premium_users_historical")
S3bucket_prime_corporate_node3.toDF().createOrReplaceTempView("prime_corporate_clients")

query = """
SELECT *,
CASE 
WHEN LENGTH(date_reviewed_raw) in (9,10) then date_format(to_date(date_reviewed_raw, 'dd/M/yyyy'),'yyyy-MM-dd')
WHEN LENGTH(date_reviewed_raw) in (6,7,8) then date_format(to_date(date_reviewed_raw, 'd/M/yy'),'yyyy-MM-dd')
ELSE null end as date_reviewed,
'premium_upgrade' as tagging
FROM (
SELECT
lower(Client_Email) as email,
ID_Submitted as submitted_id,
Proof_of_Income as proof_of_income,
Proof_of_Residency as proof_of_residency,
replace(replace(replace(trim(Date_Reviewed),'"',''),'-',''),' ','') as date_reviewed_raw,
Remarks as remarks,
RANK() OVER (PARTITION BY lower(Client_Email) ORDER BY Date_Reviewed desc) rnk1
FROM premium_users_upgrade
WHERE Remarks = 'Approved!')
WHERE rnk1=1
"""
premium_upgrade = spark.sql(query)

query = """
SELECT lower(username) as email,
name,
user_tier,
sub_group,
javi_clients,
contact_number as contact_number_raw,
CASE WHEN contact_number like '63%' THEN substr(replace(contact_number,' ',''),-10) 
ELSE replace(contact_number,' ','') END as contact_number,
LENGTH(replace(contact_number,' ','')) as contact_len,
'user_tagging' as tagging
FROM 
prime_tagging_users
"""

prime_tagging = spark.sql(query)


query = """
SELECT *,
CASE WHEN approval_date_raw = 'June-8-2021' THEN date_format(to_date('2021-08-22','yyyy-MM-dd'),'yyyy-MM-dd')
WHEN approval_date_raw = 'April-25-2021' THEN date_format(to_date('2021-04-25','yyyy-MM-dd'),'yyyy-MM-dd')
WHEN approval_date_raw = 'Jan-16,2020' THEN date_format(to_date('2020-01-16','yyyy-MM-dd'),'yyyy-MM-dd')
WHEN approval_date_raw = '14/03/20212' THEN date_format(to_date('2021-03-14','yyyy-MM-dd'),'yyyy-MM-dd')
WHEN approval_date_raw like '%-%' and length(approval_date_raw) between 10 and 11 then
date_format(to_date(approval_date_raw, 'MMM-d-yyyy'),'yyyy-MM-dd')
WHEN approval_date_raw like '%-%' and length(approval_date_raw) in (9,8) then
date_format(to_date(approval_date_raw, 'd-MMM-yy'),'yyyy-MM-dd')
WHEN approval_date_raw like ('%/%') and length(approval_date_raw) = 10 then
date_format(to_date(approval_date_raw, 'd/M/yyyy'),'yyyy-MM-dd') 
WHEN  approval_date_raw like ('%/%') and length(approval_date_raw) in (6,7) then
date_format(to_date(approval_date_raw, 'M/d/yy'),'yyyy-MM-dd') 

ELSE null END as approval_date,
length(approval_date_raw) as len_date


FROM (
SELECT full_name_fn_ln,
lower(email_address) as email,
replace(trim(approval_date),' ','-') as approval_date_raw,
'prime_historical' as tagging
FROM prime_users_historical)a
"""
prime_hist = spark.sql(query)

query = """
SELECT lower(email_address) as email,
date_approved as date_approved_raw,
date_format(to_date(date_approved, 'M/d/yy'),'yyyy-MM-dd')  as date_approved,
'premium_historical' as tagging
FROM 
premium_users_historical
"""
premium_hist = spark.sql(query)

query = """
SELECT 
Email_Address as email,
Last_Name,
First_Name,Middle_Name,
Date_of_Birth as Date_of_Birth_raw,
length(Date_of_Birth) as dob_len,
CASE WHEN length(Date_of_Birth) = 10  then date_format(to_date(Date_of_Birth, 'd/M/yyyy'),'yyyy-MM-dd') 
WHEN length(Date_of_Birth) in (6,7,8)  then date_format(to_date(Date_of_Birth, 'M/d/yy'),'19yy-MM-dd') 
ELSE null END AS date_of_birth,
Address,
Date_Endorsed,
Date_of_Approval as Date_of_Approval_raw,
length(Date_of_Approval) as doa_len,
CASE WHEN length(Date_of_Approval) = 10  then date_format(to_date(Date_of_Approval, 'd/M/yyyy'),'yyyy-MM-dd') 
WHEN length(Date_of_Approval) in (6,7,8)  then date_format(to_date(Date_of_Approval, 'M/d/yy'),'yyyy-MM-dd') 
ELSE Date_of_Approval END AS date_of_approval,
Status,
'prime_upgrade' as tagging
FROM 
prime_users_upgrade
WHERE Status = 'Approved'
"""
prime_upgrade = spark.sql(query)

query = """
SELECT 
lower(email) as email,
client_entity_name,
date_of_onboarding as date_of_onboarding_raw,
date_format(to_date(date_of_onboarding, 'M/d/yy'),'yyyy-MM-dd') as date_of_onboarding,
'prime_corporate' as tagging
FROM
prime_corporate_clients
"""
prime_corp = spark.sql(query)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
prime_upgrade.repartition(1).write.partitionBy("tagging").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/prime-users/")
premium_upgrade.repartition(1).write.partitionBy("tagging").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/prime-users/")
prime_hist.repartition(1).write.partitionBy("tagging").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/prime-users/")
premium_hist.repartition(1).write.partitionBy("tagging").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/prime-users/")
prime_tagging.repartition(1).write.partitionBy("tagging").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/prime-users/")
prime_corp.repartition(1).write.partitionBy("tagging").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/prime-users/")
job.commit()

