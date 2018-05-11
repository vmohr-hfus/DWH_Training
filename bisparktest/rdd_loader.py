from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType


def parse_row(row):
    return int(row[0]), row[1], row[2], row[3]


def create_rdd(sc, file_path, skip_header='Y'):
    # file:// tells spark to look in local file system
    rdd = sc.textFile('file://{}'.format(file_path))

    rdd = rdd.map(lambda x: x.split(','))

    if skip_header == 'Y':
        header = rdd.first()
        rdd = rdd.filter(lambda x: x != header)
        rdd = rdd.map(parse_row)

    return rdd


def load_to_rdd(args, sc, sqc):
    rdd = create_rdd(sc, args.file, args.skipheader)

    print rdd.take(args.num)


def load_to_df(args, sc, sqc):
    rdd = create_rdd(sc, args.file)

    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('email', StringType(), True)
    ])

    df = sqc.createDataFrame(rdd, schema)

    df.show(10)

    #df.registerTempTable('df')
    #
    #rdd = sqc.sql(
    #    '''
    #    SELECT count(*) AS c FROM df
    #    '''
    #)

    c = df.count()

    g = df.groupBy('country').agg(sum('value').alias())

    print rdd.first()
