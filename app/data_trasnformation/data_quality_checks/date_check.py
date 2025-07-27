from ...interfaces.transform_interface import DataTransform
import pandas as pd
import pytz


class DateCheck(DataTransform):
    def clean(self, df):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        ist = pytz.FixedOffset(330)
        df["timestamp"] = df["timestamp"].dt.tz_convert(ist)

        return df
