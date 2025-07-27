from ...interfaces.transform_interface import DataTransform


class DropMissing(DataTransform):
    def clean(self, df):
        df = df.dropna(subset=["sensor_id", "timestamp", "value"])
        return df
