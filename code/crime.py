import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession


conf = SparkConf().setAppName("crime")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)
inputfile = inputfile.mapPartitions(lambda x: reader(x))
inputfile = inputfile.filter(lambda x: x != [])

def extractor(line):
	CMPLNT_FR_DT = line[1] 
	CMPLNT_FR_TM = line[2] 
	OFNS_DESC = line[7] 




	if str(OFNS_DESC) == 'nan':  # in case of rows whose offense type is null
		OFNS_DESC = 'NotSpecified'

	
	CRM_ATPT_CPTD_CD = line[10] 

	if str(CRM_ATPT_CPTD_CD) == 'nan': # in case of rows whose zip_code is null
		CRM_ATPT_CPTD_CD = 'NotSpecified'

	LAW_CAT_CD = line [11] 
	JURIS_DESC = line[12] 
	BORO_NM = line[13] 

	if str(BORO_NM) == 'nan':  #in case of rows whose brough information is null
		BORO_NM = 'NotSpecified'

	PREM_TYP_DESC =line[16] 
	if CMPLNT_FR_DT.strip() == '':
		Year = -1
		Month = -1
		Day= -1
	else:
		Year = CMPLNT_FR_DT[6:10]
		Month = CMPLNT_FR_DT[0:2]
		Day = CMPLNT_FR_DT[3:5]
		
	key = str(Year) + ',' + str(Month) + ',' + str(Day) + ',' + str(-1) + ',' + str(2) + ',' + str(-1) + ',' + str(0)
	return (key,str(OFNS_DESC),str(CRM_ATPT_CPTD_CD),str(LAW_CAT_CD),str(JURIS_DESC),str(BORO_NM),str(PREM_TYP_DESC ))



result = inputfile.map(extractor)



## SQL
spark = SparkSession \
.builder \
.appName("crime") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

schemaString = "key OFNS_DESC CRM_ATPT_CPTD_CD LAW_CAT_CD JURIS_DESC BORO_NM PREM_TYP_DESC"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(result, schema)
schema_input.createOrReplaceTempView("result")

output=spark.sql("SELECT key, COUNT(DISTINCT PREM_TYP_DESC) as PREM_TYP_DESC,COUNT(DISTINCT JURIS_DESC) as JURIS_DESC ,COUNT(DISTINCT OFNS_DESC) as OFNS_DESC,SUM(CASE WHEN BORO_NM = 'STATEN ISLAND' then 1 else 0 end) as STATEN_ISLAND, SUM(CASE WHEN BORO_NM = 'QUEENS' then 1 else 0 end) as QUEENS, SUM(CASE WHEN BORO_NM = 'BRONX' then 1 else 0 end) as BRONX, SUM(CASE WHEN BORO_NM = 'BROOKLYN' then 1 else 0 end) as BROOKLYN ,SUM(CASE WHEN BORO_NM = 'MANHATTAN' then 1 else 0 end) as MANHATTAN, SUM(CASE WHEN CRM_ATPT_CPTD_CD ='COMPLETED' then 1 else 0 end) as COMPLETED, SUM(CASE WHEN CRM_ATPT_CPTD_CD = 'ATTEMPTED' then 1 else 0 end) as ATTEMPTED,SUM(CASE WHEN LAW_CAT_CD = 'MISDEMEANOR' then 1 else 0 end) as MISDEMEANOR, SUM(CASE WHEN LAW_CAT_CD = 'FELONY' then 1 else 0 end) as FELONY, SUM(CASE WHEN LAW_CAT_CD = 'VIOLATION' then 1 else 0 end) as VIOLATION FROM output GROUP BY key ORDER BY substring(key,1,4),substring(key,6,7),substring(key, 9,10)")
output.write.save("crime.out",format="csv")




