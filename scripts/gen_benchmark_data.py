import pyarrow as pa
import pyarrow.parquet as pq


def gen_benchmark_data():
    t = pq.read_table("../tpch-data/sf-10/orders/part.0.parquet", columns=["o_comment"])

    encodings = [
        "PLAIN",
        # "PLAIN_DICTIONARY",     # pyarrow says "it's used by default."
        # "DELTA_BINARY_PACKED",  # just int types
        "DELTA_BYTE_ARRAY",
        "DELTA_LENGTH_BYTE_ARRAY",
    ]

    # Make a single table with the three columns (one per parquet encoding)
    joined = pa.Table.from_arrays([t.column(0) for _ in encodings], names=encodings)

    writer = pq.ParquetWriter(
        "benchmark_data.parquet",
        joined.schema,
        column_encoding={name: name for name in encodings},
        use_dictionary=False,
    )
    writer.write_table(joined)
    writer.close()


if __name__ == "__main__":
    gen_benchmark_data()


