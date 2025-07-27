from ...interfaces.transform_interface import DataTransform
from scipy.stats import zscore
import numpy as np


class ZScore(DataTransform):
    def clean(self, df):
        df = (
            df.groupby("reading_type")
            .apply(self.remove_outliers)
            .reset_index(drop=True)
        )

        return df

    def remove_outliers(self, group):
        group["zscore"] = zscore(group["value"].astype(float))

        return group[np.abs(group["zscore"]) <= 3]
