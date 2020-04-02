from pyspark import SparkContext, SQLContext
from pathlib import Path

basePath = Path.cwd()
inputPath = basePath.joinpath('Inputs')
outputPath = basePath.joinpath('Outputs')
inputLogFileName = 'log_file.txt'

sc = SparkContext()
sqlc = SQLContext(sc)

rddLog = sc.textFile(str(inputPath.joinpath(inputLogFileName)))
rddLogRows = rddLog.map(lambda rows: rows.split(','))

print("Assignment 1::")
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
