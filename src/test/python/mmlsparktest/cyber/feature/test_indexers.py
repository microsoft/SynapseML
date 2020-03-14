# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest
from pyspark.sql import functions as f
from mmlspark.cyber.feature import indexers
from mmlsparktest.spark import *


class TestIndexers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.df = sc.createDataFrame(
            [
                ('1', 'a', 'A', 1, 1),
                ('1', 'b', 'A', 2, 1),
                ('1', 'a', 'B', 1, 2),
                ('2', 'aa', 'AA', 1, 1),
                ('2', 'bb', 'AA', 2, 1),
                ('3', 'b', 'B', 1, 1)
            ],
            ["tenant", "user", "res", "expected_uid", "expected_rid"]
        ).cache()

    @classmethod
    def tearDownClass(cls):
        cls.df.unpersist()

    def test_id_indexer(self):
        indexer = indexers.IdIndexer('user', 'tenant', 'actual_uid', True)
        new_df = indexer.fit(self.df).transform(self.df).cache()
        assert new_df.count() == self.df.count()
        assert 0 == new_df.filter(
            f.col('expected_uid') != f.col('actual_uid')
        ).count()

    def test_multi_indexer(self):
        multi_indexer = indexers.MultiIndexer([
            indexers.IdIndexer('user', 'tenant', 'actual_uid', True),
            indexers.IdIndexer('res', 'tenant', 'actual_rid', True),
        ])

        new_df = multi_indexer.fit(self.df).transform(self.df).cache()

        assert new_df.count() == self.df.count()
        assert new_df.filter(f.col('actual_uid') <= 0).count() == 0
        assert new_df.filter(f.col('actual_rid') <= 0).count() == 0

        assert 0 == new_df.filter(
            f.col('expected_uid') != f.col('actual_uid')
        ).count()

        assert 0 == new_df.filter(
            f.col('expected_rid') != f.col('actual_rid')
        ).count()

    def test_multi_indexer_undo_transform(self):
        multi_indexer = indexers.MultiIndexer([
            indexers.IdIndexer('user', 'tenant', 'actual_uid', True),
            indexers.IdIndexer('res', 'tenant', 'actual_rid', True),
        ])

        model = multi_indexer.fit(self.df)
        new_df = model.transform(self.df).cache()

        assert new_df.filter(f.col('actual_uid') <= 0).count() == 0
        assert new_df.filter(f.col('actual_rid') <= 0).count() == 0

        orig_df = model.undo_transform(new_df.select(
            'tenant', 'actual_uid', 'actual_rid'
        ))

        assert (orig_df
                .select('tenant', 'user')
                .distinct()
                .orderBy('tenant', 'user')
                .collect() ==
                self.df.select('tenant', 'user')
                .distinct()
                .orderBy('tenant', 'user').collect())

        assert (orig_df.select('tenant', 'res')
                .distinct()
                .orderBy('tenant', 'res')
                .collect() ==
                self.df.select('tenant', 'res')
                .distinct()
                .orderBy('tenant', 'res').collect())

    def test_multi_indexer_non_per_tenant(self):
        multi_indexer = indexers.MultiIndexer([
            indexers.IdIndexer('user', 'tenant', 'actual_uid', False),
            indexers.IdIndexer('res', 'tenant', 'actual_rid', False)
        ])

        model = multi_indexer.fit(self.df)
        new_df = model.transform(self.df).cache()

        assert new_df.count() == self.df.count()
        assert new_df.filter(f.col('actual_uid') <= 0).count() == 0
        assert new_df.filter(f.col('actual_rid') <= 0).count() == 0

        user_count = self.df.select('tenant', 'user').distinct().count()
        res_count = self.df.select('tenant', 'res').distinct().count()

        assert new_df.select('actual_uid').distinct().count() == user_count
        assert new_df.select('actual_rid').distinct().count() == res_count

        stats_row = new_df.select(
            f.min('actual_uid').alias('min_uid'),
            f.max('actual_uid').alias('max_uid'),
            f.min('actual_rid').alias('min_rid'),
            f.max('actual_rid').alias('max_rid')
        ).first()

        assert (stats_row['min_uid'] == 1) and (stats_row['max_uid'] == user_count)
        assert (stats_row['min_rid'] == 1) and (stats_row['max_rid'] == res_count)

        orig_df = model.undo_transform(new_df).select('tenant', 'user', 'res').orderBy('tenant', 'user', 'res')

        assert (self.df.select('tenant', 'user', 'res')
                .orderBy('tenant', 'user', 'res').collect() == orig_df.collect())


if __name__ == "__main__":
    result = unittest.main()
