import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession


conf = SparkConf().setAppName("collision")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)
inputfile = inputfile.mapPartitions(lambda x: reader(x))
inputfile = inputfile.filter(lambda x: x != [])

def extractor(line):
	DATE = line[0]                                   
	TIME  = line[1]                                 
	ZIP_CODE = line[3]  
	if ZIP_CODE.strip() == '':  # in case of rows whose zip_code is null
		ZIP_CODE = -1             
	PERSONS_INJURED = line[10]              
	PERSONS_KILLED = line[11]               
	PEDESTRIANS_INJURED = line[12]          
	PEDESTRIANS_KILLED = line[13]           
	CYCLIST_INJURED = line[14]              
	CYCLIST_KILLED = line[15]               
	MOTORIST_INJURED = line[16]             
	MOTORIST_KILLED = line[17]              
	Year = DATE[6:10]
	Month = DATE[0:2]
	Day = DATE[3:5]

	key = str(Year) + ',' + str(Month) + ',' + str(Day) + ',' + str(-1) + ',' + str(2) + ',' + str(ZIP_CODE) + ','+ str(1)
	return (key,str(PERSONS_INJURED),str(PERSONS_KILLED),str(PEDESTRIANS_INJURED),str(PEDESTRIANS_KILLED),str(CYCLIST_INJURED),str(CYCLIST_KILLED),str(MOTORIST_INJURED),str(MOTORIST_KILLED))


output = inputfile.map(extractor)



## SQL
spark = SparkSession \
.builder \
.appName("collision") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

schemaString = "key PERSONS_INJURED PERSONS_KILLED PEDESTRIANS_INJURED PEDESTRIANS_KILLED CYCLIST_INJURED CYCLIST_KILLED MOTORIST_INJURED MOTORIST_KILLED"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(output, schema)
schema_input.createOrReplaceTempView("output")



pair=spark.sql("SELECT key, SUM(PERSONS_INJURED) as TOTAL_PER_I, SUM(PERSONS_KILLED) as TOTAL_PER_K, SUM(PEDESTRIANS_INJURED) as TOTAL_PED_I, SUM(PEDESTRIANS_KILLED) as TOTAL_PED_K, SUM(CYCLIST_INJURED) as TOTAL_CYC_I, SUM(CYCLIST_KILLED) as TOTAL_CYC_K, SUM(MOTORIST_INJURED) as TOTAL_MOT_I, SUM(MOTORIST_KILLED) as TOTAL_MOT_K FROM output GROUP BY key ORDER BY substring(key,1,4),SUBSTRING(key, 6, 7), SUBSTRING(key, 9, 10)")
pair.write.save("collision.out",format="csv")
#pair.select(functions.format_string('%s\t%.2f, %.2f, %.2f, %.2f,%.2f, %.2f, %.2f, %.2f',pair.key, pair.TOTAL_PER_I,pair.TOTAL_PER_K, pair.TOTAL_PED_I, pair.TOTAL_PED_K, pair.TOTAL_CYC_I, pair.TOTAL_CYC_K, pair.TOTAL_MOT_I,pair.TOTAL_MOT_K)).write.save("collision.out",format="csv")
