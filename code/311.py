import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession

conf = SparkConf().setAppName("311")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)
inputfile = inputfile.mapPartitions(lambda x: reader(x))

def extractor(line):
	Date = line[1]                                   
	AgencyName=line[4] 
	ComplaintType=line[5]
	Descriptor=line[6]


	if Descriptor.strip() == '':  
		Descriptor = 'NotSpecified'

	LocationType=line[7]

	if LocationType.strip() == '':  


	IncidentZip=line[8]

	if (str(IncidentZip) == 'nan' or str(IncidentZip) == 'N/A' or IncidentZip.strip() ==''):  ### pasa those rows whose zipcode is nan 
		IncidentZip = -1

	City=line[16]

	if City.strip() == '': 
		City = 'NotSpecified'


	Status=line[19]

	if Status.strip() == '': 
		Status = 'NotSpecified'

	
	Borough=line[24]
	SchoolCity=line[35]   
	Year = Date[6:10]
	Month = Date[0:2]
	Day = Date[3:5]
	#hr = Date[11:13]

	key = str(Year) + ',' + str(Month) + ',' + str(Day) + ',' + str(-1) + ',' + str(2) + ',' + str(IncidentZip) + ',' + str(1)
	return (key,str(AgencyName),str(ComplaintType),str(Descriptor),str(LocationType),str(City),str(Status),str(Borough),str(SchoolCity))

	

output = inputfile.map(extractor)


## SQL
spark = SparkSession \
.builder \
.appName("311") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

schemaString = "key AgencyName ComplaintType Descriptor LocationType City Status Borough SchoolCity"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(output, schema)
schema_input.createOrReplaceTempView("output")



pair1=spark.sql("SELECT key, COUNT(DISTINCT AgencyName) AS cont_AgencyName, COUNT(DISTINCT ComplaintType) AS cont_ComplaintType, COUNT(DISTINCT Descriptor) AS cont_Descriptor, COUNT(DISTINCT LocationType) AS cont_LocationType, COUNT(DISTINCT City) AS cont_City, COUNT(DISTINCT Status) AS cont_Status, COUNT(DISTINCT SchoolCity) AS cont_SchoolCity, SUM(CASE WHEN Borough = 'Unspecified' THEN 1 ELSE 0 end) as cont_unspecified, SUM(CASE WHEN Borough = 'MANHATTAN' THEN 1 ELSE 0 end) as cont_MANHATTAN, SUM(CASE WHEN Borough = 'BROOKLYN'  THEN 1 ELSE 0 end) as cont_BROOKLYN, SUM(CASE WHEN Borough = 'BRONX' THEN 1 ELSE 0 end) as cont_BRONX, SUM(CASE WHEN Borough = 'QUEENS' THEN 1 ELSE 0 end) as cont_QUEENS, SUM(CASE WHEN Borough = 'STATEN ISLAND' THEN 1 ELSE 0 end) as cont_STATENISLAND FROM output GROUP BY key ORDER BY SUBSTRING(key, 1, 4), SUBSTRING(key, 6, 7),SUBSTRING(key, 9, 10)")
pair1.write.save("311.out",format="csv")


