from ...interfaces.transform_interface import DataTransform
import pandas as pd
import numpy as np


class ProfileData(DataTransform):
    def clean(self, df):
        EXPECTED_RANGES = {"temperature": (0, 35), "humidity": (20, 80)}

        def flag_anomalous(row):
            low, high = EXPECTED_RANGES.get(
                row["reading_type"], (float("-inf"), float("inf"))
            )
            return not (low <= row["value"] <= high)

        df["anomalous_reading"] = df.apply(flag_anomalous, axis=1)

        missing_pct = (
            df.groupby("reading_type")["value"]
            .apply(lambda x: x.isna().mean() * 100)
            .reset_index(name="pct_missing_values")
        )
        anomalous_pct = (
            df.groupby("reading_type")["anomalous_reading"]
            .mean()
            .mul(100)
            .reset_index(name="pct_anomalous_readings")
        )

        all_hours = pd.date_range(
            df["timestamp"].min().floor("h"), df["timestamp"].max().ceil("h"), freq="h"
        )
        expected_hours = pd.MultiIndex.from_product(
            [df["sensor_id"].unique(), all_hours], names=["sensor_id", "expected_hour"]
        )
        expected_df = pd.DataFrame(index=expected_hours).reset_index()

        actual_hours = df.copy()
        actual_hours["hour"] = actual_hours["timestamp"].dt.floor("h")
        actual_hours = actual_hours[["sensor_id", "hour"]].drop_duplicates()
        actual_hours.rename(columns={"hour": "expected_hour"}, inplace=True)

        missing_hours = expected_df.merge(
            actual_hours, on=["sensor_id", "expected_hour"], how="left", indicator=True
        )
        missing_hours = missing_hours[missing_hours["_merge"] == "left_only"]

        missing_pct_time = (
            missing_hours.groupby("sensor_id").size()
            / expected_df.groupby("sensor_id").size()
            * 100
        )
        missing_pct_time = missing_pct_time.reset_index(name="pct_time_gap_coverage")

        profile = missing_pct.merge(anomalous_pct, on="reading_type")
        sensors = df["sensor_id"].unique()
        coverage_df = pd.DataFrame({"sensor_id": sensors}).merge(
            missing_pct_time, on="sensor_id"
        )
        profile["sensor_id"] = np.nan
        profile_expanded = profile.merge(coverage_df, how="cross")
        profile_expanded["pct_time_gap_coverage"] = profile_expanded[
            "pct_time_gap_coverage"
        ].fillna(0)

        return profile_expanded
