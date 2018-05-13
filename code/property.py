import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession
conf = SparkConf().setAppName("property")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)

inputfile = inputfile.mapPartitions(lambda x: reader(x))
inputfile = inputfile.filter(lambda x: x != [])

def extractor(line):
	ass_year = line[29]
	if line[19]=='':
		zipcode = -1
	else:
		zipcode = line[19]
	lot = line[3] ## size
	build_cls = line[6] ##199classification 
	tax_cls = line[7]
	build_wid = line[21]
	build_dep = line[22]
	stories = line[11]
	market_val = line[12]
	actual_tot = line[14]

	key = str(ass_year) + ',' +str(-1) + ',' +  str(-1)+ ',' + str(-1)+ ','+ str(0) + ','+ str(zipcode)+ ',' +str(1)
	return (key, str(lot), str(build_cls), str(tax_cls), str(build_wid), str(build_dep), str(stories), str(market_val), str(actual_tot))


	output = inputfile.map(extractor)
	spark = SparkSession \
	.builder \
	.appName('property') \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()


	schemaString = "key lot_size build_cls tax_cls build_wid build_dep stories market_val actual_tot"
	fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
	schema = StructType(fields)
	schema_input = spark.createDataFrame(output, schema)
	schema_input.createOrReplaceTempView("output")
	key = spark.sql("SELECT key FROM output")


	pair=spark.sql("SELECT key, COUNT(DISTINCT build_cls) as build_cls, COUNT(DISTINCT tax_cls) as tax_cls, AVG(lot_size) as mean_lotsize, AVG(build_wid) as mean_bldgwid, AVG(build_dep) as mean_bldgdep, AVG(stories) as mean_stories, AVG(market_val) as mean_marketval, AVG(actual_tot) as mean_tol FROM output GROUP BY key ORDER BY substring(key,1)")
	pair.write.save("property.out",format="csv")

	#pair1 = spark.sql('SELECT key, build_cls, count(*) FROM output GROUP BY key,build_cls')
	#pair1.write.save("propertybuild.out",format="csv")


	#pair2 = spark.sql('SELECT key, tax_cls, count(*) FROM output GROUP BY key,tax_cls')
	#pair2.write.save("propertytax.out",format="csv")pair=spark.sql("SELECT key, COUNT(DISTINCT build_cls) as build_cls, COUNT(DISTINCT tax_cls) as tax_cls, AVG(lot_size) as mean_lotsize, AVG(build_wid) as mean_bldgwid, AVG(build_dep) as mean_bldgdep, AVG(stories) as mean_stories, AVG(market_val) as mean_marketval, AVG(actual_tot) as mean_tol FROM output GROUP BY key ORDER BY substring(key,1),substring(key,2)")
#	pair.write.save("propertyNemurical.out",format="csv")

	#pair1 = spark.sql('SELECT key, build_cls, count(*) FROM output GROUP BY key,build_cls')
	#pair1.write.save("propertybuild.out",format="csv")


	#pair2 = spark.sql('SELECT key, tax_cls, count(*) FROM output GROUP BY key,tax_cls')
	#pair2.write.save("propertytax.out",format="csv")




