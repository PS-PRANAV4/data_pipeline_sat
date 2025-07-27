from ...interfaces.transform_interface import DataTransform


class AnomalousReading(DataTransform):
    def __init__(self):
        super().__init__()
        self.EXPECTED_RANGES = {
            "temperature": (-21, 50),
            "humidity": (-1, 100),
        }

    def clean(self, df):
        df["anomalous_reading"] = df.apply(self.is_anomalous, axis=1)
        return df

    def is_anomalous(self, row):
        low, high = self.EXPECTED_RANGES.get(
            row["reading_type"], (float("-inf"), float("inf"))
        )
        return not (low <= row["calibrated_value"] <= high)
