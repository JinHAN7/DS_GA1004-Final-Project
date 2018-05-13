import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession



conf = SparkConf().setAppName("weather")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)
inputfile = inputfile.mapPartitions(lambda x: reader(x))
inputfile = inputfile.filter(lambda x: x != [])


def extractor(line):
	Date = line[0]
	Time = line[1]
	Spd = line[2].strip()
	Visb = line[3].strip()
	if Visb.strip() ==  '':
		Visb = 9999
	Temp = line[4].strip()
	Prcp = line[5].strip()
	Year = Date[0:4]
	Month = Date[4:6]
	Day = Date[6:8]
	hr = 0
	if len(Time) == 1 or len(Time) == 2:
		hr = '00'	
	elif len(Time) ==  3:
		hr = '0'+ Time[0]
	elif len(Time) == 4:
		hr = Time[0:2]
	key = str(Year) + ',' + str(Month) + ',' + str(Day) + ',' + str(hr) + ',' + str(3) + ',' + str(-1) + ',' + str(0)
	return (key,str(Spd),str(Visb),str(Temp),str(Prcp))

result = inputfile.map(extractor)


## SQL
spark = SparkSession \
.builder \
.appName("weather") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

schemaString = "key Spd Visb Temp Prcp"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(result, schema)
schema_input.createOrReplaceTempView("result")



output=spark.sql("SELECT key, AVG(Spd) as mean_Spd, AVG(Visb) as mean_Visb, AVG(Temp) as mean_Temp, AVG(Prcp) as mean_Prec FROM result GROUP BY key ORDER BY substring(key,1,4), substring(6,7), substring(9,10)")
output.write.save('weather.out',format = 'csv')





