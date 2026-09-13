import sys
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])  # RDD из строк файлов
    print(f"Lines: {lines}")
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    print(f"Counts: {counts}")  # RDD из пар (слово, количество)
    output = counts.collect()
    for word, count in output:
        print("%s: %i" % (word, count))

    spark.stop()
