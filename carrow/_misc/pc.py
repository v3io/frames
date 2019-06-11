
from pyarrow import plasma
import pyarrow as pa


def get_df(db_path, oid):
    conn: plasma.PlasmaClient = plasma.connect(db_path, '', 0)
    oid = oid.rjust(20, '0')
    oid = plasma.ObjectID(oid.encode())
    print('getting', oid.binary())

    if oid not in conn.list():
        raise KeyError(f'error: unknown ID: {oid.binary()}')
    buf, = conn.get_buffers([oid])
    reader = pa.RecordBatchStreamReader(buf)
    record_batch: pa.RecordBatch = reader.read_next_batch()
    return record_batch.to_pandas()


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description='read from plasma')
    parser.add_argument('db_path', help='db path')
    parser.add_argument('oid', help='object id')
    args = parser.parse_args()

    df = get_df(args.db_path, args.oid)
    print(df)