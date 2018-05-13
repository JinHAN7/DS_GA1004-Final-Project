import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession
conf = SparkConf().setAppName("bike")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)

inputfile = inputfile.mapPartitions(lambda x: reader(x))
inputfile = inputfile.filter(lambda x: x != [])

def extractor(line):
	
	tripduration=line[0]###by second
	startTime = line[1]
	
	startStation = line[4]
	
	endStation  = line[8]
	if str(endStation) == 'nan':  
		endStation = 'NotSpecified'

	
	usertype=line[12]
	birthYear=line[13]


	
	if str(birthYear)=='nan':  
		birthYear = 'NotSpecified'
	


	gender=line[14] ##count  

	 		#total_amount = line[18]
	if len(startTime)==19:
		if startTime[0]==1:
			startYear=startTime.split('/')[2][0:4]
			startMonth=startTime.split('/')[0]
			starDate=startTime.split('/')[1]

		else:
			startYear= startTime[0:4]
			startMonth = startTime[5:7]
			startDate = startTime[8:10]
		

		#startHour = startTime[11:13]



	else:
		startYear= startTime.split('/')[2][0:4]
		if len(startTime.split('/')[0])==1:
			startMonth = "0"+startTime.split('/')[0]

		else:
			startMonth=startTime.split('/')[0]

		if len(startTime.split('/')[1])==1:
			startDate = "0"+startTime.split('/')[1]
		else:
			startDate=startTime.split('/')[1]
		
		#startHour = startTime[11:13]
	
	
	key = str(startYear) + ',' + str(startMonth) + ',' + str(startDate)  + "," + str(-1) + "," + str(2) + "," + str(-1) + ',' + str(0)
	return (key,str(tripduration),str(startStation),str(endStation),str(usertype),str(birthYear),str(gender))
	



output = inputfile.map(extractor)

spark = SparkSession \
.builder \
.appName("bike") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()



schemaString = "key tripduration startStation endStation usertype birthYear gender"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(output, schema)
schema_input.createOrReplaceTempView("output")
key = spark.sql("SELECT key FROM output")





pair=spark.sql("SELECT key,COUNT(DISTINCT endStation) as endStation,COUNT(DISTINCT birthYear) as birthYear, COUNT(DISTINCT startStation) as startStation,AVG(tripduration) as mean_tripDuration, SUM(CASE WHEN usertype ='Customer' then 1 else 0 end) as Customer, SUM(CASE WHEN usertype = 'Subscriber' then 1 else 0 end) as Subscriber, SUM(CASE WHEN gender = 0 then 1 else 0 end) as unknown, SUM(CASE WHEN gender = 1 then 1 else 0 end) as male, SUM(CASE WHEN gender = 2 then 1 else 0 end) as female FROM output GROUP BY key ORDER BY substring(key,1,4),substring(key,6,7),substring(key, 9,10)")
pair.write.save("bike.out",format="csv")







