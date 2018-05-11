from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType

if __name__ == '__main__':
    with SparkContext(appName='My Spark Application') as sc:

        sql_context = SQLContext(sc)
    
        rdd = sc.parallelize([
            ("Xe:", 5),
            ("Socs:", 6),
            ("Xe:", 7),
            ("Socs:", 8),
            ("Xe:", 19),
            ("Xe:", 7)
        ])

        rdd = rdd.reduceByKey(lambda a, b: a + b)

        schema = StructType([
            StructField("name", StringType()),
            StructField("age", IntegerType())
        ])
        print rdd.take(2)

        df = sql_context.createDataFrame(rdd, schema)
        print df.show()