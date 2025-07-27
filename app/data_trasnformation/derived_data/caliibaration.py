from ...interfaces.transform_interface import DataTransform


class Caliboration(DataTransform):
    def __init__(self):
        super().__init__()
        self.CALIBRATION_PARAMS = {
            "temperature": {"multiplier": 0.1, "offset": -2},
            "humidity": {"multiplier": -1.95, "offset": 0},
        }

    def clean(self, df):
        df["calibrated_value"] = df.apply(self.calibrate, axis=1)
        return df

    def calibrate(self, row):
        params = self.CALIBRATION_PARAMS.get(
            row["reading_type"], {"multiplier": 1, "offset": 0}
        )
        return row["value"] * params["multiplier"] + params["offset"]
