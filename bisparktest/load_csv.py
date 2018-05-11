import argparse

from pyspark import SparkContext
from pyspark import SQLContext

from bisparktest.rdd_loader import load_to_df, load_to_rdd


def config_parser():
    parser = argparse.ArgumentParser(description='Load CSV File')
    parser.add_argument(
        '--file',
        dest='file',
        help='Path to CSV File',
        required=True
    )

    sub_parsers = parser.add_subparsers()

    rdd_parser = sub_parsers.add_parser('rdd')
    rdd_parser.set_defaults(func=load_to_rdd)
    rdd_parser.add_argument(
        '--num',
        dest='num',
        type=int,
        default=5,
        help='Number of values to print',
    )

    rdd_parser.add_argument(
        '--skip-header',
        dest='skipheader',
        choices=['Y', 'N'],
        default='Y'
    )

    df_parser = sub_parsers.add_parser('df')
    df_parser.set_defaults(func=load_to_df)

    return parser


parser = config_parser()

if __name__ == '__main__':
    args = parser.parse_args()

    with SparkContext(appName='Load Users From CSV') as sc:
        sqc = SQLContext(sc)
        args.func(args, sc, sqc)
