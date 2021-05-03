"""
pyspark

from stitchr_extensions.wrangle import *

"""

# import os
# from pyspark.sql.functions import concat_ws, collect_list
# import typing

import pyspark
from pyspark.sql.types import StructField, StructType, ArrayType
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame

# import typing_extensions

import sys
from random import choice
from string import ascii_letters
import re

spark = (pyspark.sql.SparkSession.builder.getOrCreate())
spark.sparkContext.setLogLevel('WARN')

"""spark = SparkSession.builder.getOrCreate()
# import spark.implicits._
spark.sparkContext.setLogLevel('WARN')
"""
print(sys.path)

""" setting up the path to include Stitchr and other project related imports"""


# sys.path.append(os.environ['STITCHR_ROOT'] + '/pyspark-app/app')

# print("Spark Version:", spark.sparkContext.version)
def left_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """

    :param left_df:
    :param right_df:
    :return:
    """
    left_columns_set = set(left_df.schema.fieldNames())
    right_columns_set = set(right_df.schema.fieldNames())
    # what is the toSet on python? .toSet
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(left_columns_set - right_columns_set)


def right_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """

    :param left_df:
    :param right_df:
    :return:
    """
    left_columns_set = set(left_df.schema.names)
    right_columns_set = set(right_df.schema.names)
    # what is the toSet on python? .toSet
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(right_columns_set - left_columns_set)


def select_list(df: DataFrame, column_list: list) -> DataFrame:
    """

    :param df:
    :param column_list:
    :return:
    """
    cl: list = f"`{'`,`'.join(column_list)}`".split(',')
    return df.select(*cl)


def select_exclude(df: DataFrame, columns_2_exclude: list) -> DataFrame:
    """
    :param df:
    :param columns_2_exclude:
    :return:
    """
    column_list: list = list(set(df.schema.fieldNames()) - set(columns_2_exclude))
    return df.select(*column_list)


def drop_columns(df: DataFrame, drop_columns_list: list) -> DataFrame:
    """

    :param drop_columns_list:
    :param df:
    :return:
    """
    df_columns_set = set(df.schema.fieldNames())
    # warn that some columns are not in the list... Or maybe throw an error?
    cols_that_do_not_exist = set(drop_columns_list) - df_columns_set
    # get the actual list of columns to drop
    columns_2_remove = list(set(drop_columns_list) - cols_that_do_not_exist)
    # drop and return
    return df.drop(*columns_2_remove)


# too long as a line need to figure out how to wrap the lambda function in multi-line code?
"""
def drop_columns(drop_columns_list: list, df: DataFrame) -> DataFrame:
    
    return df.drop(*list(set(drop_columns_list) - (set(drop_columns_list) - set(df.schema.fieldNames()))))

"""


def rename_columns(df: DataFrame, rename_mapping_dict: dict) -> DataFrame:
    """
    :param df:
    :param rename_mapping_dict:
    :return: DataFrame
    Takes a dictionary of columns to be renamed and returns a converted dataframe
    """
    # we use iteration over the dict and call df.WithColumn
    # this is not efficient?
    df_columns: list = df.schema.fieldNames()
    df_new_columns: list = [rename_mapping_dict[f] if (f in rename_mapping_dict)
                            else f
                            for f in df_columns]
    return df.toDF(*df_new_columns)


# def rename_column(existing: str, new: str, df: DataFrame) -> DataFrame:
def rename_column(df: DataFrame, existing: str, new: str) -> DataFrame:
    """Returns a new :class:`DataFrame` by renaming an existing column.
    This is a no-op if schema doesn't contain the given column name.
    Effectively a wrapper over withColumnRenamed

    :rtype: object
    :param existing: string, name of the existing column to rename.
    :param new: string, new name of the column.

    >>> df.rename_column('age', 'age2').collect()
    [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
    """
    return df.withColumnRenamed(existing, new)


def rename_4_parquet(df: DataFrame) -> DataFrame:
    """
    rename all columns of the dataFrame so that we can save as a Parquet file
    :param df:
    :return:
    """
    # need to add left/right trims and replace multiple __ with one?
    # r = "[ ,;{}()\n\t=]"
    # added "." and "-" and / so that we skip using ``
    # regex = r"[ ,;{}()\n\t=.-]"
    regex = r"[ ,;{}()\n\t=.-/]"
    delimiter = '__'
    f = df.schema.fields
    return spark.createDataFrame(
        df.rdd,
        StructType(
            [StructField(re.sub(regex, delimiter, sf.name.replace(' ', '')), sf.dataType, sf.nullable) for sf in
             f]
            )
       )


def unpivot(df: DataFrame, unpivot_keys: list,
            unpivot_column_list: list,
            key_column: str = "key_column",
            value_column: str = "value") -> DataFrame:
    """

    :param df:
    :param unpivot_keys:
    :param unpivot_column_list:
    :param key_column:
    :param value_column:
    :return:
    """
    # we can improve by checking the parameter lists to be in the schema
    stack_fields_array = unpivot_column_list
    # we need to cast to STRING as we may have int, double , etc... we would couple this with extracting the types
    pivot_map_list = [f"'{s.replace('`', '')}', STRING({(s)}) " for s in stack_fields_array]
    stack_fields = f"stack({len(stack_fields_array)},{','.join([str(x) for x in pivot_map_list])})"
    df.createOrReplaceTempView("_unpivot")
    q = f"select {','.join([str(x) for x in unpivot_keys])}, {stack_fields} as (`{key_column}`, `{value_column}`) from _unpivot"
    # replace with logging ... print(f'''query is: {q} \n''')
    return spark.sql(q)


def flatten(data_frame: DataFrame) -> DataFrame:
    """
    NH: Experimental
    :param data_frame:
    :return:
    """
    fields: StructType = data_frame.schema.fields
    field_names: list = data_frame.schema.fieldNames()
    # exploded_df = data_frame
    for index, value in enumerate(fields):
        field = value
        field_type = field.dataType
        field_name = field.name
        # print(f'{field}, {field_name}, {field_type}')
        if isinstance(field_type, ArrayType):
            field_names_excluding_array = [fn for fn in field_names if fn != field_name]
            field_names_to_select = field_names_excluding_array + [
                f"explode_outer({field_name}) as {field_name}"]
            # exploded_df = exploded_df.selectExpr(*field_names_to_select)
            # return flatten(exploded_df)
            return flatten(data_frame.selectExpr(*field_names_to_select))
        elif isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", "__")) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # return flatten(exploded_df)
            return flatten(data_frame.select(*renamed_cols))
    # print(f'schema size is {len(data_frame.schema.fieldNames())}')
    return data_frame


def flatten_no_explode(data_frame: DataFrame) -> DataFrame:
    """
    NH: experimental ...still under test.... may explode single element arrays (which may also be acceptable)
    :param data_frame:
    :return:
    """
    fields: StructType = data_frame.schema.fields
    field_names: list = data_frame.schema.fieldNames()
    # print(len(field_names))
    # exploded_df = data_frame
    for index, value in enumerate(fields):
        field = value
        field_type = field.dataType
        field_name = field.name
        # print(f'{field}, {field_name}, {field_type}')
        if isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            print(f'{field_name}, {child_fieldnames}')
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", "__")) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # exploded_df.printSchema()
            # return flatten_no_explode(exploded_df)
            return flatten_no_explode(data_frame.select(*renamed_cols))
    # print(f'schema size is {len(data_frame.schema.fieldNames())}')
    return data_frame


@property
def get_random_string(length: int) -> str:
    """
    Random string with the combination of lower and upper case
    :param length:
    :return:
    """
    letters = ascii_letters
    result_str = ''.join(choice(letters) for _ in range(length))
    return result_str


def add_column(new_column: str, transform, df: DataFrame):
    """
    :param transform:
    :param new_column:
    :param df:
    :param :
    :return:

    """
    # stub holder
    # Assuming all columns are correct... But we better add a check step similar to the drop columns function
    return df


# this class may be deprecated if we end up having 2 inependent code lines (scala and python)
# class DfExtensions(DataFrame):
class DfExtensions:
    """
    set of transforms that invoke stitchr extensions
    wrapper around teh scala implementation and interfaces
    NH 9/17/20: need to pull out the DF reference and make it a parameter
    """

    # from pyspark DataFrame ...
    def __init__(self, jdf, sql_ctx):
        # DataFrame(jdf, sql_ctx)
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc
        self.is_cached = False
        self._schema = None  # initialized lazily
        self._lazy_rdd = None
        # Check whether _repr_html is supported or not, we use it to avoid calling _jdf twice
        # by __repr__ and _repr_html_ while eager evaluation opened.
        self._support_repr_html = False
        # assert isinstance(df, object)
        # DataFrame(df)

    def left_diff_schemas(self, right_df: DataFrame) -> list:
        """
        still is done inside pyspark... need to modify to invoke the gateway wrapper
        :param right_df:
        :return:
        """
        left_columns_set = set(self.df.schema.names)
        right_columns_set = set(right_df.schema.names)
        # warning: some columns are not in the list... maybe throw a warning error?
        return list(left_columns_set - right_columns_set)

    def right_diff_schemas(self, right_df: DataFrame) -> list:
        """
        still is done inside pyspark... need to modify to invoke the gateway wrapper
        :param right_df:
        :return:
        """
        left_columns_set = set(self.df.schema.names)
        right_columns_set = set(right_df.schema.names)
        # warning: some columns are not in the list... maybe throw a warning error?
        return list(right_columns_set - left_columns_set)

    def drop_columns(self, drop_columns_list: list):
        """

        :param drop_columns_list:
        :return:
        """
        df_columns_set = set(self._jdf.schema.names)
        # warning: some columns are not in the list... maybe throw a warning error?
        cols_that_donot_exist = set(drop_columns_list) - df_columns_set
        # get the actual list of columns to drop
        columns2remove = list(set(drop_columns_list) - cols_that_donot_exist)
        # drop and return
        # return self._jdf.drop(*columns2remove)
        # testing 4/9/21
        return DfExtensions(self._jdf.drop(*columns2remove))

    def rename_columns(self, rename_mapping_dict: dict):
        """
        :param rename_mapping_dict:
        :return: DataFrame
        Takes a dictionary of columns to be renamed and returns a converted dataframe
        Uses the thin wrapper around spark scala with Py4J
        """
        # return DfExtensions(self.sql_ctx._jvm.com.stitchr.extensions.transform.Dataframe.renameColumns(
        #    _dict_to_scala_map(self._sc, rename_mapping_dict)), self._jdf, self.sql_ctx)
        # return self.sql_ctx._jvm.com.stitchr.extensions.transform.Dataframe.renameColumns(_dict_to_scala_map(self._sc, rename_mapping_dict), self._jdf)
        # return DfExtensions(self.sql_ctx._jvm.com.stitchr.extensions.transform.Dataframe.renameColumns(_dict_to_scala_map(self._sc, rename_mapping_dict)), self.sql_ctx)
        return DfExtensions(self._sc._jvm.com.stitchr.extensions.transform.Df.renameColumns(
            _dict_to_scala_map(self._sc, rename_mapping_dict))(self._jdf), self._sc)

    def rename_column(self, existing: str, new: str):
        """Returns a new :class:`DataFrame` by renaming an existing column.
        This is a no-op if schema doesn't contain the given column name.

        :param existing: string, name of the existing column to rename.
        :param new: string, new name of the column.

        >>> df.withColumnRenamed('age', 'age2').collect()
        [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
        """
        # either calls work...
        #
        # return DfExtensions(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)
        return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)


def _dict_to_scala_map(sc, jm):
    """
    Convert a dict into a JVM Map.
    """
    return sc._jvm.PythonUtils.toScalaMap(jm)


def transform0(self, f):
    """
    pyspark does not have a transform before version 3... we need to add one to DataFrame.
    This is based on https://mungingdata.com/pyspark/chaining-dataframe-transformations/
    """
    return f(self)


def _test():
    """
    test code
    :return:
    """
    import os
    from pyspark.sql import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("sql.catalog tests") \
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    globs['spark'] = spark
    # ...


DataFrame.transform0 = transform0

if __name__ == "__main__":
    print('running tests \n')
    _test()
