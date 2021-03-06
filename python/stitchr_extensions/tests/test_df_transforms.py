"""
stitchr_extensions tests

"""
import unittest
from resources import data
from stitchr_extensions.df_transforms import *


class TestWrangleMethods(unittest.TestCase):

    def test_left_diff_schemas(self):
        left_df: DataFrame = data.test_df
        right_df:DataFrame = data.test_df1
        left_diff: list = left_diff_schemas(left_df, right_df)
        # "K E   Y", "cols with   sp  aces", " .value"
        self.assertEqual(left_diff, [" .value"])

    def test_right_diff_schemas(self):
        left_df = data.test_df
        right_df = data.test_df1
        right_diff = right_diff_schemas(left_df, right_df)
        self.assertEqual(right_diff, ["value"])

    def test_select_list(self):
        df = select_list(data.test_df, ["K E   Y", " .value"])
        self.assertEqual(df.schema.fieldNames(), ["K E   Y", " .value"])
        self.assertEqual(df.count(), 3)

    def test_select_exclude(self):
        df = select_exclude(data.test_df1, ["cols with   sp  aces", "value"])
        self.assertEqual(df.schema.fieldNames(), ["K E   Y"])
        self.assertEqual(df.count(), 3)

    def test_drop_columns(self):
        df = drop_columns(data.test_df1, ["cols with   sp  aces", "value"])
        self.assertEqual(df.schema.fieldNames(), ["K E   Y"])
        self.assertEqual(df.count(), 3)

    def test_rename_columns(self):
        rename_mapping_dict = {"K E   Y": "key",
                               "cols with   sp  aces": "col1",
                               " .value": "val"
                               }
        df = rename_columns(data.test_df, rename_mapping_dict)
        self.assertEqual(df.schema.fieldNames(), ['key', 'col1', 'val'])
        self.assertEqual(df.count(), 3)

    def test_rename_4_parquet(self):
        """
        input schema is "K E   Y", "cols with   sp  aces", " .value"
        :return:
        """
        df = rename_4_parquet(data.test_df)
        # replace with logging print(df.schema.fieldNames())
        self.assertEqual(df.schema.fieldNames(), ['KEY', 'colswithspaces', '__value'])
        self.assertEqual(df.count(), 3)

    def test_unpivot(self):
        data.simple_df.printSchema()
        # ["firstname", "middlename", "lastname", "id", "location", "salary"]
        df_unpivot = unpivot(data.simple_df, ["firstname", "lastname"], ["id", "location", "salary"])
        # the following would be logging and we need to add asserts
        df_unpivot.printSchema()
        df_unpivot.show()

    def test_flatten(self):
        df_out = flatten(data.df_json)
        self.assertEqual(len(df_out.schema.fieldNames()), 10)
        self.assertEqual(df_out.count(), 2)

    def test_flatten_no_explode(self):
        df_out = flatten_no_explode(data.df_json)
        self.assertEqual(len(df_out.schema.fieldNames()), 9)
        self.assertEqual(df_out.count(), 1)



if __name__ == '__main__':
    unittest.main()

