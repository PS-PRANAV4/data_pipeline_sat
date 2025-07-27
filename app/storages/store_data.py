import os


class StoreData:
    def store(self, df, base_path, compression, partition_cols):
        os.makedirs(base_path, exist_ok=True)

        df.to_parquet(
            base_path,
            engine="pyarrow",
            compression=compression,
            partition_cols=partition_cols,
            index=False,
        )
