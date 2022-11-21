import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env','dateDelay'])
env = args['env']
dateDelay = args['dateDelay']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
pDate_filter = "partition_0 == year(date_sub(current_date, "+dateDelay+")) AND partition_1 == month(date_sub(current_date, "+dateDelay+")) AND partition_2 == day(date_sub(current_date, "+dateDelay+"))"

# Script generated for node S3 bucket
masterdata_source_node1 = glueContext.create_dynamic_frame.from_catalog(
#    database="data_team_osl_db",
#    table_name="osl_kyc_reports",
    push_down_predicate= pDate_filter,
    database="pdax_data_infra_reports",
    table_name="kyc_reports",
    transformation_ctx="masterdata_source_node1",
)

# Script generated for node ApplyMapping
masterdata_mapping_node2 = ApplyMapping.apply(
    frame=masterdata_source_node1,
    mappings=[
        ("email", "string", "email", "string"),
        ("user_id", "string", "user_id", "string"),
        ("guid", "string", "guid", "string"),
        ("tier", "string", "tier", "string"),
        ("verification_state", "string", "verification_state", "string"),
        ("first_name", "string", "first_name", "string"),
        ("middle_name", "string", "middle_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("name_suffix", "string", "name_suffix", "string"),
        ("sex", "string", "sex", "string"),
        ("contact_no", "string", "contact_no", "string"),
        ("birthdate", "string", "birthdate", "string"),
        ("birth_country", "string", "birth_country", "string"),
        ("birth_city", "string", "birth_city", "string"),
        ("nationality", "string", "nationality", "string"),
        ("address", "string", "address", "string"),
        ("city", "string", "city", "string"),
        ("region", "string", "region", "string"),
        ("country", "string", "country", "string"),
        ("zipcode", "string", "zipcode", "string"),
        ("income_source", "string", "income_source", "string"),
        ("submitted_id", "string", "submitted_id", "string"),
        ("application_id", "string", "application_id", "string"),
        ("verification_id", "string", "verification_id", "string"),
        ("status", "string", "status", "string"),
        ("error", "string", "error", "string"),
        ("verified_by", "string", "verified_by", "string"),
        ("email_template_sent", "string", "email_template_sent", "string"),
        ("status_updated_at", "string", "status_updated_at", "string"),
        ("created_at", "string", "created_at", "string"),
        ("updated_at", "string", "updated_at", "string"),
        ("country_code", "string", "country_code", "string"),
        ("address_line_1", "string", "address_line_1", "string"),
        ("address_line_2", "string", "address_line_2", "string"),
        ("partition_0", "string", "partition_0", "string"),
        ("partition_1", "string", "partition_1", "string"),
        ("partition_2", "string", "partition_2", "string"),
    ],
    transformation_ctx="masterdata_mapping_node2",
)
masterdata_mapping_node2.toDF().createOrReplaceTempView("master_clients")

query = """
SELECT
email,
user_id,
guid,
tier,
verification_state,
first_name,
middle_name,
last_name,
name_suffix,
CASE 
    WHEN UPPER(sex) LIKE ('F%') THEN 'Female'
    WHEN UPPER(sex) LIKE ('M%') THEN 'Male' else null
END AS sex,
CASE
    WHEN UPPER(contact_no) LIKE 'PH%' THEN '+63' 
    ELSE contact_no 
END AS country_code,
CASE 
    WHEN contact_no = '+63' and (birthdate like '63%' AND length(birthdate) < 10) THEN replace(birthdate,'63','')
    WHEN contact_no = '+63' and (length(birthdate) < 10 and birthdate like '+%') THEN replace(replace(birthdate,'+63',''),'+','')
    WHEN contact_no = '+63' THEN substr(birthdate,-10)
    WHEN contact_no LIKE 'PH%' THEN substr(birthdate,-10)
    WHEN contact_no != '+63' and length(birthdate) < 10 THEN birthdate
    WHEN contact_no in ('+998','+974','+852') and length(birthdate) > 10 THEN substr(birthdate,4)
    ELSE birthdate 
END AS contact_number,
CASE
    WHEN (birth_country = '-' or UPPER(birth_country) = 'INVALID DATE' or birth_country = '0' or birth_country = '') THEN null
    WHEN length(birth_country) = 10 THEN date_format(to_date(birth_country, 'yyyy-MM-dd'),'yyyy-MM-dd')
    WHEN length(birth_country) = 23 THEN date_format(to_date(birth_country, 'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd')
    WHEN length(birth_country) = 9 THEN date_format(to_date(concat('1',birth_country), 'yyyy-MM-dd'),'yyyy-MM-dd')
    WHEN length(birth_country) = 8 THEN date_format(to_date(birth_country, 'yy-MM-dd'),'yyyy-MM-dd')
    WHEN length(birth_country) = 7 THEN date_format(to_date(birth_country, 'M-yy-dd'),'yyyy-MM-dd')
    ELSE null
END AS birthdate,
UPPER(birth_city) as birth_country,
regexp_replace(regexp_replace(UPPER(nationality),'[.,+/#!$%^&*;:{}=_`~()-\]',''),'[0-9]','') as birth_city,
CASE 
   WHEN UPPER(address) LIKE 'FILIPINO %' AND UPPER(address) != ('FILIPINO CITIZEN') THEN 'FILIPINO (DUAL)'
   WHEN upper(income_source) like 'PH%' AND (UPPER(address) IN ('F','FI','FILIPINO CITIZEN','PINOY','PL','ASIAN','MANILA','BISAYA','NCR')
    OR (UPPER(address) LIKE '%CITY%' OR UPPER(address) LIKE 'NATION%' OR UPPER(address) LIKE '%TAGALOG%')) THEN 'FILIPINO'
   WHEN (UPPER(address) LIKE 'FH%'
      or UPPER(address) LIKE 'PH%' 
      or UPPER(address) LIKE 'F%INO'
      or UPPER(address) LIKE 'F%NO'
      or UPPER(address) LIKE 'P%INO'
      or UPPER(address) LIKE '%LIPI%'
      or UPPER(address) LIKE 'FIL%'
      or UPPER(address) LIKE 'PIL%'
      or UPPER(address) LIKE '%PPINES'
      ) THEN 'FILIPINO'
 WHEN UPPER(income_source) like 'PH%' AND length(address) != 0 THEN 'N/A'
    ELSE regexp_replace(regexp_replace(UPPER(address),'[.,+/#!$%^&*;:{}=_`~()-\]',''),'[0-9]','') 
END AS nationality,
city as address,
region as address_line_1,
country	as address_line_2,
REPLACE(regexp_replace(UPPER(zipcode),'[.,+/#!$%^&*;:{}=_`~()-]',''),'\','') as region,
CASE 
    WHEN upper(income_source) in ('-','MOBILE COUNTRY CODE') then null
    WHEN upper(income_source) like 'PH%' then 'PH'
    ELSE upper(income_source) 
END AS country,
CASE
    WHEN UPPER(TRIM(submitted_id)) IN
        ('NONE','NOT APPLICABLE','N/A','NA','NILL','NO ZIP CODE','OOOO','1','O','X','0','00','000','0000','DELETED')
        OR UPPER(TRIM(submitted_id)) LIKE '%00000%' THEN 'N/A'
    WHEN upper(income_source) LIKE 'PH%' AND LENGTH(TRIM(submitted_id)) >= 10 THEN 'N/A'
    WHEN upper(income_source) LIKE 'PH%' THEN 
        TRIM(replace(replace(REGEXP_REPLACE(REGEXP_REPLACE(submitted_id,'[a-zA-Z]+',''),'[._,()/# ]+',''),'-',''),'+63',''))
    WHEN upper(income_source) NOT LIKE 'PH%' AND UPPER(TRIM(submitted_id)) LIKE '+%' THEN 'N/A'
    ELSE TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(UPPER(submitted_id),'[._,()/ ]+',''),'SELECT',''),'-',''),'POBOX',''),'RIYADH',''),'000000',''))
END as zipcode,
application_id as income_source,
verification_id	as submitted_id,
status as application_id,
error as verification_id,
verified_by as status,
email_template_sent as	error,
status_updated_at as verified_by,
created_at as email_template_sent,
updated_at as status_updated_at,
country_code as created_at,
address_line_1 as updated_at,
date_sub(current_date,"""+dateDelay+""") as date_delay,
partition_0,
partition_1,
partition_2,
concat(partition_0,'-',partition_1,'-',partition_0) as p_date
FROM master_clients
"""

final_df = spark.sql(query)
#data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")
#data_dynamicframe.show()

#AmazonRedshift_node5 = glueContext.write_dynamic_frame.from_catalog(
#    frame=data_dynamicframe,
#    database="spectrumdb_pdax_data_"+env+"",
#    table_name="spectrumdb_pdax_data_"+env+"_master_clients_data_"+env+"",
#    redshift_tmp_dir=args["TempDir"],
#    transformation_ctx="AmazonRedshift_node5",
#)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
final_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/master_data_kyc_dev/")

job.commit()