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
        "paths": ["s3://pdax-data-dev-trxn-pull-staging/manual_files/pdax_prime_user/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_prime_user_node0",
)

# Script generated for node S3 bucket
masterdata_source_node1 = glueContext.create_dynamic_frame.from_catalog(
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
        ("country_code", "string", "country_code", "string"),
        ("contact_no", "string", "contact_no", "string"),
        ("birthdate", "string", "birthdate", "string"),
        ("birth_country", "string", "birth_country", "string"),
        ("birth_city", "string", "birth_city", "string"),
        ("nationality", "string", "nationality", "string"),
        ("address_line_1", "string", "address_line_1", "string"),
        ("address_line_2", "string", "address_line_2", "string"),
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
        ("address", "string", "address", "string"),
        ("partition_0", "string", "partition_0", "string"),
        ("partition_1", "string", "partition_1", "string"),
        ("partition_2", "string", "partition_2", "string"),
    ],
    transformation_ctx="masterdata_mapping_node2",
)
S3bucket_country_node0.toDF().createOrReplaceTempView("country_code_2022")
masterdata_mapping_node2.toDF().createOrReplaceTempView("master_clients")
S3bucket_prime_user_node0.toDF().createOrReplaceTempView("prime_users")
query = """
SELECT email,user_id,guid,
case when clients.email = lower(prime.username) then prime.user_tier else clients.tier end as tier,
prime.sub_group,prime.javi_clients,
verification_state,first_name,middle_name,last_name,name_suffix,sex,country_code,
case when clients.email = lower(prime.username) then replace(prime.contact_number,' ','') else clients.contact_no end as contact_no,
birthdate,birth_country,birth_city,nationality,region,address_line_1,
address_line_2,city,country,iso_country_code,zipcode,income_source,
submitted_id,application_id,verification_id,status,error,verified_by,
email_template_sent,status_updated_at,created_at,updated_at,
date_delay,p_date,cnt1, date_delay2,
CAST(CURRENT_TIMESTAMP() as timestamp) as curr_timestamp,
RANK() OVER (PARTITION BY email ORDER BY created_at desc) rnk1,
RANK() OVER (PARTITION BY email ORDER BY updated_at desc) rnk2
FROM (
SELECT
lower(email) as email,
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
    WHEN UPPER(country_code) LIKE 'PH%' THEN '63' 
    ELSE country_code 
END AS country_code,
CASE 
    WHEN country_code = '+63' and (contact_no like '63%' AND length(contact_no) < 10) THEN replace(contact_no,'63','')
    WHEN country_code = '+63' and (length(contact_no) < 10 and contact_no like '+%') THEN replace(replace(contact_no,'+63',''),'+','')
    WHEN country_code = '+63' THEN substr(contact_no,-10)
    WHEN country_code LIKE 'PH%' THEN substr(contact_no,-10)
    WHEN country_code != '+63' and length(contact_no) < 10 THEN contact_no
    WHEN country_code in ('+998','+974','+852') and length(contact_no) > 10 THEN substr(contact_no,4)
    ELSE contact_no 
END AS contact_no,
CASE
    WHEN (birthdate = '-' or UPPER(birthdate) = 'INVALID DATE' or birthdate = '0' or birthdate = '') THEN null
    WHEN length(birthdate) = 10 THEN date_format(to_date(birthdate, 'yyyy-MM-dd'),'yyyy-MM-dd')
    WHEN length(birthdate) = 23 THEN date_format(to_date(birthdate, 'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd')
    WHEN length(birthdate) = 9 THEN date_format(to_date(concat('1',birthdate), 'yyyy-MM-dd'),'yyyy-MM-dd')
    WHEN length(birthdate) = 8 THEN date_format(to_date(birthdate, 'yy-MM-dd'),'yyyy-MM-dd')
    WHEN length(birthdate) = 7 THEN date_format(to_date(birthdate, 'M-yy-dd'),'yyyy-MM-dd')
    ELSE null
END AS birthdate,
CASE WHEN upper(c1.birth_country) like 'PH%' THEN 'PH'
     WHEN upper(c1.birth_country) = c3.country_name then c3.alpha_2_code
     WHEN upper(c1.birth_country) IN ('SOUTH KOREA','KOREA SOUTH') THEN 'KR'
     WHEN upper(c1.birth_country) = 'UNITED STATES' THEN 'US'
     ELSE upper(c1.birth_country) 
END AS birth_country,
regexp_replace(regexp_replace(UPPER(birth_city),'[.,+/#!$%^&*;:{}=_`~()-\]',''),'[0-9]','') as birth_city,
CASE 
   WHEN UPPER(nationality) LIKE 'FILIPINO %' AND UPPER(nationality) != ('FILIPINO CITIZEN') THEN 'FILIPINO (DUAL)'
   WHEN upper(income_source) like 'PH%' AND (UPPER(nationality) IN ('F','FI','FILIPINO CITIZEN','PINOY','PL','ASIAN','MANILA','BISAYA','NCR')
    OR (UPPER(nationality) LIKE '%CITY%' OR UPPER(nationality) LIKE 'NATION%' OR UPPER(nationality) LIKE '%TAGALOG%')) THEN 'FILIPINO'
   WHEN (UPPER(nationality) LIKE 'FH%'
      or UPPER(nationality) LIKE 'PH%' 
      or UPPER(nationality) LIKE 'F%INO'
      or UPPER(nationality) LIKE 'F%NO'
      or UPPER(nationality) LIKE 'P%INO'
      or UPPER(nationality) LIKE '%LIPI%'
      or UPPER(nationality) LIKE 'FIL%'
      or UPPER(nationality) LIKE 'PIL%'
      or UPPER(nationality) LIKE '%PPINES'
      ) THEN 'FILIPINO'
 WHEN UPPER(country) like 'PH%' AND length(nationality) != 0 THEN 'N/A'
    ELSE regexp_replace(regexp_replace(UPPER(nationality),'[.,+/#!$%^&*;:{}=_`~()-\]',''),'[0-9]','') 
END AS nationality,
address_line_1,
address_line_2,
city,
REPLACE(regexp_replace(UPPER(region),'[.,+/#!$%^&*;:{}=_`~()-]',''),'\','') as region,
CASE WHEN upper(c1.country) like 'PH%' THEN 'PHILIPPINES'
     WHEN upper(c1.country) = c2.country_name THEN upper(c1.country)
     WHEN upper(c1.country) = c2.alpha_2_code THEN c2.country_name
     WHEN upper(c1.country) = c2.alpha_3_code THEN c2.country_name
     WHEN upper(c1.country) in ('-','MOBILE COUNTRY CODE') THEN '-'
     ELSE upper(c1.country)
END AS country,
CASE WHEN upper(c1.country) like 'PH%' THEN 'PH'
     WHEN upper(c1.country) = c2.country_name THEN c2.alpha_2_code
     WHEN upper(c1.country) = c2.alpha_2_code THEN c2.alpha_2_code
     WHEN upper(c1.country) = c2.alpha_3_code THEN c2.alpha_2_code
     WHEN upper(c1.country) in ('-','MOBILE COUNTRY CODE') THEN '-'
     WHEN upper(c1.country) IN ('SOUTH KOREA','KOREA SOUTH') THEN 'KR'
     WHEN upper(c1.country) = 'UNITED STATES' THEN 'US'
ELSE upper(c1.country)
END as iso_country_code,
CASE
    WHEN UPPER(TRIM(zipcode)) IN
        ('NONE','NOT APPLICABLE','N/A','NA','NILL','NO ZIP CODE','OOOO','1','O','X','0','00','000','0000','DELETED')
        OR UPPER(TRIM(zipcode)) LIKE '%00000%' THEN 'N/A'
    WHEN upper(country) LIKE 'PH%' AND LENGTH(TRIM(zipcode)) >= 10 THEN 'N/A'
    WHEN upper(country) LIKE 'PH%' THEN 
        TRIM(replace(replace(REGEXP_REPLACE(REGEXP_REPLACE(zipcode,'[a-zA-Z]+',''),'[._,()/# ]+',''),'-',''),'+63',''))
    WHEN upper(country) NOT LIKE 'PH%' AND UPPER(TRIM(zipcode)) LIKE '+%' THEN 'N/A'
    ELSE TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(UPPER(zipcode),'[._,()/ ]+',''),'SELECT',''),'-',''),'POBOX',''),'RIYADH',''),'000000',''))
END as zipcode,
upper(income_source) as income_source,
submitted_id,
application_id,
verification_id,
status,
error,
verified_by,
email_template_sent,
status_updated_at,
cast(created_at  as timestamp) as created_at,
updated_at,
date_sub(current_date,"""+dateDelay+""") as date_delay,
concat(partition_0,'-',partition_1,'-',partition_2) as p_date,
COUNT(upper(income_source)) OVER (PARTITION BY upper(income_source)) AS cnt1,
cast(date_sub(current_date,("""+dateDelay+""" + 1)) as timestamp) as date_delay2
FROM master_clients c1
LEFT JOIN country_code_2022 c2
ON (upper(c1.country) = c2.country_name
OR upper(c1.country) = c2.alpha_2_code
OR upper(c1.country) = c2.alpha_3_code)
left join country_code_2022 c3
ON upper(c1.birth_country) = c3.country_name
OR upper(c1.birth_country) = c3.alpha_2_code) clients
LEFT JOIN prime_users prime 
ON clients.email = lower(prime.username)
"""
#cast(updated_at as timestamp) as updated_at,
masters_temp = spark.sql(query).createOrReplaceTempView("master_clients_temp")

query="""
SELECT
email,
user_id,
guid,
tier,
sub_group,
javi_clients,
verification_state,
first_name,
middle_name,
last_name,
name_suffix,
sex,
country_code,
contact_no,
birthdate,
birth_country,
birth_city,
nationality,
address_line_1,
address_line_2,
city,
region,
country,
iso_country_code,
zipcode,
CASE 
WHEN (income_source LIKE '%EMPLOY%' OR income_source LIKE 'ACCOUNT%' or income_source LIKE 'ADMIN%')
or income_source LIKE 'WORK%' OR income_source LIKE 'BPO%' OR income_source LIKE 'CALL%' OR income_source LIKE 'COMPANY%'
or income_source IN ('JOB','WORK','WORKING','BPO','WORKER','COMPANY','CLERK') THEN 'EMPLOYED'
WHEN income_source LIKE '%ONLINE%' THEN 'OTHERS ONLINE SELLING'
WHEN income_source LIKE '%PROFESSION%' or income_source ='HOMEBASED' OR income_source LIKE '%SELF EMPLOYED'
OR income_source LIKE '%TECH%' OR income_source LIKE 'SELLING%' OR income_source LIKE '%FREELAN%' THEN 'SELF-EMPLOYED'
WHEN (income_source LIKE '%ALLOW%' OR income_source IN('ALLAWANCE','ALOWANCE','ALLAWONCE','ALLLOWANCE') ) THEN 'ALLOWANCE'
WHEN (income_source LIKE '%CRYPTO%' or income_source LIKE '%AXIE%' OR income_source LIKE '%COIN%' OR income_source LIKE 'CRYT%'
OR income_source LIKE '%NFT%' or income_source LIKE '%BINANCE%' or income_source LIKE '%BITC%' or income_source LIKE 'BLOCK%') 
OR income_source IN ('CRIPTO','CRPYTO','CRYTO')
THEN 'CRYPTO TRADING AND INVESTMENTS'
WHEN income_source LIKE '% SALARY' THEN 'OTHERS SALARY'
WHEN income_source LIKE '%AGRI%' THEN 'AGRICULTURE'
WHEN income_source LIKE '%BUSINESS%' THEN 'BUSINESS INCOME'
WHEN (income_source LIKE '%BUY%' AND income_source LIKE '%SELL%') THEN 'OTHERS BUY AND SELL'
WHEN income_source LIKE '%-RENT' or income_source like '%RENTAL' or income_source LIKE '%HOUSE RENTAL%' OR
income_source in ('RENT HOUSE','RENTA','RENTALS','HOUSE UNIT RENTAL','RENTAL OF RESIDENTIAL UNITS','RESIDENTIAL RENTAL')
THEN 'INCOME FROM RENTS'
WHEN cnt1 = 1 THEN 'OTHER SOURCES'
WHEN income_source like '%00' or income_source ='-' or (cnt1 <= 3 AND LENGTH(income_source) <= 3) THEN 'OTHER SOURCES'
ELSE income_source
END AS income_source,
submitted_id,
application_id,
verification_id,
status,
error,
verified_by,
email_template_sent,
status_updated_at,
CASE WHEN created_at is NULL THEN date_delay2 ELSE created_at END AS created_at,
updated_at,
curr_timestamp + INTERVAL 8 HOUR AS processdatetimestamp,
p_date
FROM master_clients_temp
WHERE rnk1 = 1 and rnk2 = 1
"""

#CASE WHEN created_at is NULL THEN date_delay2 ELSE created_at END AS 

final_df = spark.sql(query)
data_dynamicframe = DynamicFrame.fromDF(final_df.repartition(1), glueContext, "data_dynamicframe")
#data_dynamicframe.show()

#AmazonRedshift_node5 = glueContext.write_dynamic_frame.from_catalog(
#    frame=data_dynamicframe,
#    database="spectrumdb_pdax_data_"+env+"",
#    table_name="spectrumdb_pdax_data_"+env+"_master_clients_data_"+env+"",
#    redshift_tmp_dir=args["TempDir"],
#    transformation_ctx="AmazonRedshift_node5",
#)

#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#final_df.repartition(1).write.partitionBy("p_date").mode("overwrite").option("header","true").csv("s3://pdax-data-"+env+"-trxn-pull-staging/master_data_kyc_dev/")

# Script generated for node Amazon Redshift
pre_query = """drop table if exists pdax_data_dev.master_clients_kyc_data_stg;
create table pdax_data_dev.master_clients_kyc_data_stg as select * from pdax_data_dev.master_clients_kyc_data where 1=2;"""

post_query = """begin;
delete from pdax_data_dev.master_clients_kyc_data using pdax_data_dev.master_clients_kyc_data_stg
where pdax_data_dev.master_clients_kyc_data_stg.email = pdax_data_dev.master_clients_kyc_data.email 
and pdax_data_dev.master_clients_kyc_data_stg.updated_at != pdax_data_dev.master_clients_kyc_data.updated_at;

delete from pdax_data_dev.master_clients_kyc_data_stg using pdax_data_dev.master_clients_kyc_data
where pdax_data_dev.master_clients_kyc_data.email = pdax_data_dev.master_clients_kyc_data_stg.email 
and pdax_data_dev.master_clients_kyc_data.updated_at = pdax_data_dev.master_clients_kyc_data_stg.updated_at;

insert into pdax_data_dev.master_clients_kyc_data select * from pdax_data_dev.master_clients_kyc_data_stg;
drop table pdax_data_dev.master_clients_kyc_data_stg; 
end;"""


AmazonRedshift_redshift_load_node5 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=data_dynamicframe,
    catalog_connection="glue-to-redshift",
    connection_options={
        "database": "spectrumdb",
        "dbtable": "pdax_data_dev.master_clients_kyc_data_stg",
        "preactions": pre_query,
        "postactions": post_query,
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_redshift_load_node5",
)


job.commit()
