"""Shared helpers for the StreamSense notebooks.

The notebooks are still the main entry point, but a small shared module keeps
paths, feature lists, and repeated ML logic in one place.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence, TYPE_CHECKING

import pandas as pd

try:
    from pyspark.sql import DataFrame as SparkDataFrame
except ModuleNotFoundError:  # pragma: no cover - lets local smoke tests run without Spark
    SparkDataFrame = Any  # type: ignore[assignment]

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame

DEFAULT_DATA_PATH = "dbfs:/Volumes/workspace/my_catalog/my_volume/Netflix Dataset.csv"
RAW_TABLE_NAME = "netflix_raw"
CLEAN_TABLE_NAME = "netflix_clean"
EXPERIMENT_NAME = "/Shared/StreamSense_Experiments"
HIT_DESCRIPTION_LENGTH_THRESHOLD = 120

NUMERIC_FEATURES: tuple[str, ...] = ("release_year", "duration_num", "is_movie")
CATEGORICAL_FEATURES: tuple[str, ...] = ("category", "rating", "country")
FEATURE_COLUMNS: tuple[str, ...] = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def build_prediction_frame(
    *,
    category: str,
    rating: str,
    release_year: int,
    duration_num: int,
    is_movie: int,
    country: str,
    feature_columns: Sequence[str] = FEATURE_COLUMNS,
) -> pd.DataFrame:
    """Build a single-row dataframe for inference."""

    data = {
        "category": [category],
        "rating": [rating],
        "release_year": [release_year],
        "duration_num": [duration_num],
        "is_movie": [is_movie],
        "country": [country],
    }
    return pd.DataFrame(data, columns=list(feature_columns))


def predict_hit_probability(
    model,
    *,
    category: str,
    rating: str,
    release_year: int,
    duration_num: int,
    is_movie: int,
    country: str,
) -> tuple[float, int]:
    """Return the predicted hit probability and class for a hypothetical title."""

    input_df = build_prediction_frame(
        category=category,
        rating=rating,
        release_year=release_year,
        duration_num=duration_num,
        is_movie=is_movie,
        country=country,
    )
    proba = float(model.predict_proba(input_df)[0, 1])
    pred_class = int(model.predict(input_df)[0])
    return proba, pred_class


def feature_importance_frame(
    model,
    *,
    numeric_features: Sequence[str] = NUMERIC_FEATURES,
    categorical_features: Sequence[str] = CATEGORICAL_FEATURES,
) -> pd.DataFrame:
    """Extract a sorted feature-importance dataframe from the trained pipeline."""

    preprocessor = model.named_steps["preprocessor"]
    ohe = preprocessor.named_transformers_["cat"]
    encoded_cat_features = ohe.get_feature_names_out(categorical_features)
    all_features = list(numeric_features) + list(encoded_cat_features)

    classifier = model.named_steps["classifier"]
    importances = classifier.feature_importances_

    return (
        pd.DataFrame({"feature": all_features, "importance": importances})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )


def hit_rate_table(
    df_spark: SparkDataFrame,
    group_col: str,
    *,
    drop_nulls: bool = False,
    sort_descending: bool = True,
) -> SparkDataFrame:
    """Compute a count + hit-rate aggregation for a Spark dataframe."""

    from pyspark.sql import functions as F

    table = (
        df_spark.groupBy(group_col)
        .agg(
            F.avg("is_hit").alias("hit_rate"),
            F.count("*").alias("count"),
        )
    )

    if drop_nulls:
        table = table.filter(F.col(group_col).isNotNull())

    sort_col = F.col("hit_rate").desc() if sort_descending else F.col("hit_rate").asc()
    return table.orderBy(sort_col)


def save_hit_rate_plot(
    hit_rate_table_df: SparkDataFrame,
    *,
    group_col: str,
    title: str,
    output_path: str,
    kind: str = "bar",
    xlabel: str | None = None,
    ylabel: str = "Hit rate",
    figsize: tuple[int, int] = (8, 4),
    rotation: int = 30,
) -> None:
    """Save a simple hit-rate plot next to the notebook."""

    import matplotlib.pyplot as plt

    pdf = hit_rate_table_df.toPandas()
    pdf = pdf[pdf[group_col].notna()].copy()

    if kind == "bar":
        pdf[group_col] = pdf[group_col].astype(str)
        plot_fn = plt.bar
    elif kind == "line":
        pdf = pdf.sort_values(group_col)
        plot_fn = plt.plot
    else:
        raise ValueError(f"Unsupported plot kind: {kind}")

    plt.figure(figsize=figsize)

    if kind == "bar":
        plot_fn(pdf[group_col], pdf["hit_rate"])
        plt.xticks(rotation=rotation, ha="right")
    else:
        plot_fn(pdf[group_col], pdf["hit_rate"], marker="o")

    plt.xlabel(xlabel or group_col.replace("_", " ").title())
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(Path(output_path), dpi=180, bbox_inches="tight")
    plt.show()


def save_dashboard_tables(tables: Mapping[str, SparkDataFrame]) -> None:
    """Persist multiple Spark dataframes as Delta tables."""

    for table_name, table_df in tables.items():
        (
            table_df.write.mode("overwrite")
            .format("delta")
            .saveAsTable(table_name)
        )
