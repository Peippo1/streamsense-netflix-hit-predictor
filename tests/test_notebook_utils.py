from __future__ import annotations

import sys
import types
import unittest
from dataclasses import dataclass
from unittest.mock import patch

import numpy as np

from streamsense.notebook_utils import (
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    build_prediction_frame,
    feature_importance_frame,
    hit_rate_table,
    predict_hit_probability,
    save_dashboard_tables,
)


class FakeModel:
    def predict_proba(self, input_df):
        self.last_input = input_df
        return np.array([[0.2, 0.8]])

    def predict(self, input_df):
        self.last_input = input_df
        return [1]


class FakeCategoricalTransformer:
    def get_feature_names_out(self, categorical_features):
        return [f"{feature}_A" for feature in categorical_features]


@dataclass
class FakeClassifier:
    feature_importances_: list[float]


class FakePipeline:
    def __init__(self):
        self.named_steps = {
            "preprocessor": types.SimpleNamespace(
                named_transformers_={"cat": FakeCategoricalTransformer()}
            ),
            "classifier": FakeClassifier([0.3, 0.2, 0.5, 0.1, 0.2, 0.4]),
        }


class FakeWriter:
    def __init__(self):
        self.calls = []

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def saveAsTable(self, value):
        self.calls.append(("saveAsTable", value))
        return self


class FakeTable:
    def __init__(self):
        self.write = FakeWriter()


class FakeGroupedTable:
    def __init__(self):
        self.calls = []

    def agg(self, *args, **kwargs):
        self.calls.append(("agg", args, kwargs))
        return self

    def filter(self, *args, **kwargs):
        self.calls.append(("filter", args, kwargs))
        return self

    def orderBy(self, *args, **kwargs):
        self.calls.append(("orderBy", args, kwargs))
        return self


class FakeSparkFrame:
    def __init__(self):
        self.grouped = FakeGroupedTable()
        self.groupby_calls = []

    def groupBy(self, group_col):
        self.groupby_calls.append(group_col)
        return self.grouped


class NotebookUtilsTests(unittest.TestCase):
    def test_build_prediction_frame_uses_expected_columns(self):
        frame = build_prediction_frame(
            category="Movie",
            rating="TV-MA",
            release_year=2024,
            duration_num=120,
            is_movie=1,
            country="United States",
        )

        self.assertEqual(list(frame.columns), list(FEATURE_COLUMNS))
        self.assertEqual(frame.iloc[0]["category"], "Movie")
        self.assertEqual(frame.iloc[0]["release_year"], 2024)

    def test_predict_hit_probability_returns_probability_and_class(self):
        model = FakeModel()

        proba, pred_class = predict_hit_probability(
            model,
            category="Movie",
            rating="TV-MA",
            release_year=2024,
            duration_num=120,
            is_movie=1,
            country="United States",
        )

        self.assertEqual(proba, 0.8)
        self.assertEqual(pred_class, 1)
        self.assertEqual(model.last_input.columns.tolist(), list(FEATURE_COLUMNS))

    def test_feature_importance_frame_sorts_descending(self):
        frame = feature_importance_frame(
            FakePipeline(),
            numeric_features=NUMERIC_FEATURES,
            categorical_features=CATEGORICAL_FEATURES,
        )

        self.assertEqual(frame.iloc[0]["importance"], 0.5)
        self.assertTrue(frame["importance"].is_monotonic_decreasing)

    def test_hit_rate_table_uses_groupby_and_ordering(self):
        fake_sql = types.ModuleType("pyspark.sql")

        class FakeExpr:
            def __init__(self, name):
                self.name = name

            def alias(self, _):
                return self

            def desc(self):
                return ("desc", self.name)

            def asc(self):
                return ("asc", self.name)

            def isNotNull(self):
                return ("isNotNull", self.name)

        fake_functions = types.SimpleNamespace(
            avg=lambda _: FakeExpr("avg"),
            count=lambda _: FakeExpr("count"),
            col=lambda name: FakeExpr(name),
        )
        fake_sql.functions = fake_functions

        fake_pyspark = types.ModuleType("pyspark")
        fake_pyspark.sql = fake_sql

        with patch.dict(sys.modules, {"pyspark": fake_pyspark, "pyspark.sql": fake_sql}):
            from streamsense.notebook_utils import hit_rate_table as imported_hit_rate_table

            frame = FakeSparkFrame()
            result = imported_hit_rate_table(frame, "rating")

        self.assertIs(result, frame.grouped)
        self.assertEqual(frame.groupby_calls, ["rating"])
        self.assertTrue(any(call[0] == "orderBy" for call in frame.grouped.calls))

    def test_save_dashboard_tables_writes_each_table(self):
        tables = {"one": FakeTable(), "two": FakeTable()}

        save_dashboard_tables(tables)

        self.assertIn(("mode", "overwrite"), tables["one"].write.calls)
        self.assertIn(("format", "delta"), tables["one"].write.calls)
        self.assertIn(("saveAsTable", "one"), tables["one"].write.calls)
        self.assertIn(("saveAsTable", "two"), tables["two"].write.calls)


if __name__ == "__main__":
    unittest.main()
