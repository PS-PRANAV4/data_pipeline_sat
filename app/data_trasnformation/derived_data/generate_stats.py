from ...interfaces.transform_interface import DataTransform
import pandas as pd


class DeriveStats(DataTransform):
    def clean(self, df):
        df["date"] = df["timestamp"].dt.date

        df["date"] = df["timestamp"].dt.date

        daily_avg = (
            df.groupby(["sensor_id", "reading_type", "date"])["value"]
            .mean()
            .reset_index()
            .rename(columns={"value": "daily_avg_value"})
        )
        daily_avg = daily_avg.sort_values(by=["sensor_id", "reading_type", "date"])

        daily_avg["date"] = pd.to_datetime(daily_avg["date"])

        daily_avg.set_index("date", inplace=True)

        daily_avg["rolling_7d_avg"] = daily_avg.groupby(["sensor_id", "reading_type"])[
            "daily_avg_value"
        ].transform(lambda x: x.rolling(window=7, min_periods=1).mean())

        return daily_avg
