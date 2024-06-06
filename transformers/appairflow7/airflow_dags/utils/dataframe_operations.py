import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def apply_schema(file_path, schema):
    df = pd.read_json(file_path)
    for col, dtype in schema.items():
        if dtype.lower() == "string":
            df[col] = df[col].astype(str)
        elif dtype.lower().startswith("varchar"):
            df[col] = df[col].astype(str)
        elif dtype.lower().startswith("float"):
            df[col] = df[col].astype(float)
    return df


def write_to_parquet(df, output_path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    return output_path
