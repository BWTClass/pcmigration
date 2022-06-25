from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Utils:
    def schema_generate(self, src_object_nm):
        """
        Generate schema
        :param src_object_nm: provide source object name
        :return: structtypeschema
        """
        try:
            schema_lst = []
            for line in open("../../config/{}_schema.csv".format(src_object_nm), "r").readlines():
                print("line : {}".format(line))
                val_type = line.split(",")[1]
                val_name = line.split(",")[0]
                if val_type.lower() == "int":
                    schema_lst.append(StructField(val_name, IntegerType()))
                elif val_type.lower() == "string":
                    schema_lst.append(StructField(val_name, StringType()))
            return StructType(schema_lst)
        except Exception as e:
            print("Failed to execute schema_generate() method. with ERROR: {}.".format(e))