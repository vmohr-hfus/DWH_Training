from pyspark import SparkContext

if __name__ == '__main__':
    with SparkContext(appName='Word count') as sc:
        # file:// tells spark to look in local file system
        rdd = sc.textFile('file:///hellofresh/data/words.txt')

        rdd = rdd.map(lambda x: (x, 1))
        rdd.reduceByKey(lambda x, y: x + y)

        rdd.saveAsTextFile('file:///hellofresh/data/wc_result')

