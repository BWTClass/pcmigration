from pyspark.sql import SparkSession
import argparse
import sys
sys.path.insert(0, "../../scripts")
from util.logger import Logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    # retrieving arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_gcs_bucket",
                        required=True,
                        help="Source file input GCS bucket name")
    parser.add_argument("--src_object_nm",
                        required=True,
                        help="Source Object Name")
    parser.add_argument("--input_file_header",
                        required=False,
                        default=False,
                        help="Source Object Name")
    parser.add_argument("--input_file_format",
                        required=False,
                        default='csv',
                        help="Source Object Name")

    # assign input values from/using arguments
    args = parser.parse_args()
    input_gcs_bucket = args.input_gcs_bucket
    src_object_nm = args.src_object_nm
    input_file_header = args.input_file_header
    input_file_format = args.input_file_format

    # Importing logging
    # logger = Logger.logging(log_path=str(src_object_nm) + '.log')
    # logger.info("Started processing for source object : {}".format(src_object_nm))

    spark = SparkSession.builder.master('local[*]').appName("Spark Validation").getOrCreate()
    # spark.conf.set("google.cloud.auth.service.account.json.keyfile", "D:\\vaulted-bazaar-345605-ef79ba849d09.json")

    if not str(input_gcs_bucket).startswith("gs://"):
        input_gcs_bucket = "gs://{}".format(input_gcs_bucket)

    # if input_file_header:
    #     inputdf = spark.read \
    #         .format('{}'.format(input_file_format)) \
    #         .option("header", "{}".format(input_file_header)) \
    #         .load("{}/{}/*".format(input_gcs_bucket, src_object_nm))
    # else:
    #     inputdf = spark.read \
    #         .format('{}'.format(input_file_format)) \
    #         .load("{}/{}/*".format(input_gcs_bucket, src_object_nm))

    # create sparkschema
    schema_lst = []
    for line in open("../../config/{}_schema.csv".format(src_object_nm), "r").readlines():
        print("line : {}".format(line))
        val_type = line.split(",")[1]
        val_name = line.split(",")[0]
        if val_type.lower() == "int":
            schema_lst.append(StructField(val_name, IntegerType()))
        elif val_type.lower() == "string":
            schema_lst.append(StructField(val_name, StringType()))

    schema = StructType(schema_lst)
