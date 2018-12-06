import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def substr_func(x,n):
    return x[0:n]

def agentgethttp_func(str):
    ind = str.rfind('.')
    return str[1:ind]	

def auid_func(str):
 a = [' ', '&']
 ind1 = str.find('auid=')
 if ind1 != -1:
  ind2 = min(str.find(i) for i in a if i in str)
  return str[ind1+5:ind2]
 else:
  return ''

def scid_func(str):
 ind1 = str.find('scid=')
 if ind1 != -1:
  return str[ind1+5:]
 else:
  return ''

def keyword_func(str):
 a = ['&']
 ind1 = str.find('agf=')
 if ind1 != -1:
  ind2 = min(str.find(i) for i in a if i in str)
  return str[ind1+4:ind2]
 else:
  return ''

def keyword_id_func(str):
 a = ['&']
 ind1 = str.find('agf=')
 if ind1 != -1:
  ind2 = min(str.find(i) for i in a if i in str)
  return str[ind2+3:]
 else:
  return ''
 
def extract_url(str):
 if((str is None) or (len(str) == 0)):
  return ''
 else:
  return str.split("//")[-1].split("/")[0].split('?')[0]

def agent_browser(str):
 if((str is None) or (len(str) == 0)):
  return ''
 else:
  return str.replace(' ','/').split('/')[:1]
 
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "sample_db", table_name = "sample1_input", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sample_db", table_name = "sample1_input", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("cookieid", "string", "cookieid", "string"), ("tdate", "string", "tdate", "string"), ("tstamp", "string", "tstamp", "string"), ("tzone", "string", "tzone", "string"), ("verb", "string", "verb", "string"), ("request", "string", "request", "string"), ("httpversion", "string", "httpversion", "string"), ("response", "string", "response", "string"), ("size", "string", "size", "string"), ("referrer", "string", "referrer", "string"), ("agent", "string", "agent", "string")], transformation_ctx = "applymapping1")


dfnonull = DropNullFields.apply(frame = applymapping1, transformation_ctx = "dfnonull")
dfnonull.printSchema()
dfnonull.show()

sdp_df_t1 = dfnonull.resolveChoice(specs = [('tdate','cast:string'),('size','cast:string'),('referrer','cast:string')])

agentgethttp_func1 = udf(lambda x: x[1:x.find('.')], StringType())
auid_func1 = udf(auid_func)
scid_func1 = udf(scid_func)
keyword_func1 = udf(keyword_func)
keyword_id_func1 = udf(keyword_id_func)
extract_url1 = udf(extract_url)
agent_browser1 = udf(agent_browser)


for i in df.cols:
sdf_t1=df_t1.filter("cookieid is not null").withColumn("year", substr_func(df_t1["tdate"],4)).withColumn("month", substr_func(df_t1["tdate"],7)).withColumn("agentgethttp", agentgethttp_func1(sdp_df_t1["request"])).withColumn("auid", auid_func1(df_t1["request"])).withColumn("scid", scid_func1(df_t1["request"])).withColumn("keyword", keyword_func1(df_t1["request"])).withColumn("keyword_id", keyword_id_func1(df_t1["request"])).withColumn("url", extract_url1(df_t1["referrer"])).withColumn("agent_browser", agent_browser1(df_t1["agent"]))


df_t=df_t1.createOrReplaceTempView("sdp_df_tbl")

sdp_df_sql = spark.sql("""SELECT cookieid, year, month, tdate as day, 
tstamp, tzone, verb, agentgethttp, auid, scid, keyword, keyword_id, request, httpversion, response, size, url, agent_browser, referrer, agent FROM df_tbl""")


df_sql = DynamicFrame.fromDF(df_sql, glueContext, "df")
df.show()
datasink1 = glueContext.write_dynamic_frame.from_options(frame = sdp_dynf_final, connection_type = "s3", connection_options = {"path": "s3://data-sources-test/output", "partitionKeys": ["year", "month", "day"]}, format = "csv", transformation_ctx = "datasink1")

job.commit()
