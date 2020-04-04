from pyspark import SparkContext, SQLContext, sql
from pyspark.sql import types
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import expr
from pyspark.sql.functions import udf
from pathlib import Path

basePath = Path.cwd()
inputPath = basePath.joinpath('Inputs')
outputPath = basePath.joinpath('Outputs')
inputLogFileName = 'log_file.txt'

sc = SparkContext()
sqlc = SQLContext(sc)
ssc = StreamingContext(sc, 10)

rddLog = sc.textFile(str(inputPath.joinpath(inputLogFileName)))
rddLogRows = rddLog.map(lambda rows: rows.split(','))
'''
print("Spark RDD API Assignments")
print("Assignment 1:")
print("a. Used the log_file as input to created following RDD:")
print(rddLog.collect())

print("b. Filtered out records where package = “NA” and version = “NA” as follows: ")
print(rddLogRows
      .filter(lambda row: row if row[6] != 'NA' and row[7] != 'NA' else False)
      .collect())

print("c. Total number of downloads for each package is as follows: ")
rddRes = rddLogRows.groupBy(lambda row: row[6])
for res in rddRes.collect():
    if res[0] != '"package"':
        package = sc.parallelize(res[1])
        print("Package:", res[0], "is downloaded", str(package.count()), "times.")

print("d. Average download size for each Country is as follows: ")
rddRes = rddLogRows.groupBy(lambda row: row[8])
for res in rddRes.collect():
    total, avg = 0, 0
    if res[0] != '"country"':
        country = sc.parallelize(res[1])
        for rec in country.collect():
            total = total + int(rec[2])
        avg = total / country.count()
        print("Average download size for country:", res[0], "is:", str(avg))

'''
print("Spark SQL Assignments")
print("Assignment 3:")
print("a. Used the log_file as input to created following RDD:")
print(rddLog.collect())

print("b. Converting the RDD to a Data Frame:")
schema = types.StructType([types.StructField('date', types.StringType(), True),
                           types.StructField('time', types.StringType(), True),
                           types.StructField('size', types.StringType(), True),
                           types.StructField('r_version', types.StringType(), True),
                           types.StructField('r_arch', types.StringType(), True),
                           types.StructField('r_os', types.StringType(), True),
                           types.StructField('package', types.StringType(), True),
                           types.StructField('version', types.StringType(), True),
                           types.StructField('country', types.StringType(), True),
                           types.StructField('ip_id', types.StringType(), True)])
dfs = sqlc.createDataFrame(rddLogRows, schema)

# dfs = dfs.filter((dfs.date != '"date"') & (dfs.time != '"time"') &
#                 (dfs.size != '"size"') & (dfs.r_version != '"r_version"') &
#                 (dfs.r_arch != '"r_arch"') & (dfs.r_os != '"r_os"') &
#                 (dfs.package != '"package"') & (dfs.version != '"version"') &
#                 (dfs.country != '"country"') & (dfs.ip_id != '"ip_id"'))

dfs = dfs.filter((dfs.date != '"date"'))
dfs.printSchema()
dfs.show()
'''
print("c. Adding a new column to the Data Frame called 'Download_Type': ")
def valueToDownloadType(size):
    if int(size) < 100000: return 'Small'
    elif int(size) > 100000 and int(size) < 1000000: return 'Medium'
    else: return 'Large'


udfValueToDownloadType = udf(valueToDownloadType, types.StringType())
dfsWithDownloadType = dfs.withColumn("Download_Type", udfValueToDownloadType("size"))
dfsWithDownloadType.printSchema()
dfsWithDownloadType.show()

print("d. Total number of Downloads by 'Country' and 'Download Type': ")
dfsCountry = dfsWithDownloadType.groupBy("country").count().sort('count')
dfsDownloadType = dfsWithDownloadType.select(dfsWithDownloadType.Download_Type)\
    .groupBy("Download_Type").count().sort('count')

dfsCountry.show()
dfsDownloadType.show()

print("e. Saving above output as a Parquet File: ")
if not Path.exists(outputPath.joinpath("Country_Count")):
    dfsCountry.write.parquet(str(outputPath.joinpath("Country_Count")))
else:
    pass
print("'Country' data stored at location: ", outputPath.joinpath("Country_Count"))

if not Path.exists(outputPath.joinpath("Download_Type_Count")):
    dfsDownloadType.write.parquet(str(outputPath.joinpath("Download_Type_Count")))
else:
    pass
print("'Download Type' data stored at location: ", outputPath.joinpath("Download_Type_Count"))
'''

print("Spark SQL Assignments")
print("Assignment 4:")
print("a. Reading the log_file and converting to a Data Frame to save as JSON File: ")
dfs.printSchema()
dfs.show()

jsonFile = outputPath / 'JSON_File'
if not Path.exists(jsonFile):
    dfs.coalesce(1).write.format('json').save(str(jsonFile))
else:
    pass
print("Converted JSON file is stored at location: ", jsonFile)

print("b. Reading the JSON file into a data frame: ")
df = sqlc.read.json(f"{str(jsonFile)}\\*.json")
#df = sqlc.read.json(f"{str(outputPath)}\\tes.json")
print(df.first())

print("c. Perform the following transformations using Data Frame API")
print("    i.   Group By Date: ", df.groupBy('date'))

print("    ii.  Min, Max and Average Download Size for each Date: ")
dfStat = df.select(df.date, df.size.cast('int'))
#dfStat.printSchema()
#print(dfStat.rdd.reduceByKey(lambda x, y: min(int(x), int(y))).collect())
print("\t\t Min Download Size for each Date: ")
print("\t\t", dfStat.rdd.reduceByKey(lambda x, y: min(x, y)).collect())
#print(dfStat.rdd.reduceByKey(lambda x, y: max(int(x), int(y))).collect())
print("\t\t Max Download Size for each Date: ")
print("\t\t", dfStat.rdd.reduceByKey(lambda x, y: max(x, y)).collect())
#print(dfStat.rdd.aggregateByKey(lambda x, y: (int(x), int(y))).collect())
print("\t\t Avg Download Size for each Date: ")
totalsBySize = dfStat.rdd.mapValues(lambda x: (int(x), 1))\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesBySize = totalsBySize.mapValues(lambda x: x[0] / x[1])
results = averagesBySize.collect()
for result in results:
    print("\t\t", result)

print("    iii. Sorting records by Date in Ascending Order: ")
df.sort('date').take(5)

print("    iv.  Saving the output as a JSON file: ")
jsonFileSorted = outputPath / 'JSON_File_Sorted'
if not Path.exists(jsonFileSorted):
    df.coalesce(1).write.format('json').save(str(jsonFileSorted))
else:
    pass
print("Sorted JSON file is stored at location: ", jsonFileSorted)

print("Spark Streaming Assignment")
print("Assignment 5:")
lines = ssc.socketTextStream("localhost", 12)
words = lines.map(lambda rows: rows.split(','))
pairs = words.filter(lambda row: row if row[6] != 'NA' and row[7] != 'NA' else False)
pairs.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate






