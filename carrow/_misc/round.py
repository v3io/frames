import pyarrow as pa
from pyarrow import plasma
import pandas as pd
import numpy as np


def mkoid(n):
    n = str(n).rjust(20, '0').encode('ascii')
    return plasma.ObjectID(n)


df = pd.DataFrame(np.arange(12).reshape(4, 3), columns=['a', 'b', 'c'])
table: pa.Table = pa.Table.from_pandas(df)
sink = pa.MockOutputStream()
writer = pa.RecordBatchStreamWriter(sink, table.schema)
for batch in table.to_batches():
    writer.write_batch(batch)
writer.close()
print(sink.size())

# oid = mkoid(8)

oid = plasma.ObjectID(np.random.bytes(20))
print(oid)

client: plasma.PlasmaClient = plasma.connect('/tmp/plasma.db', '', 3)
buf = client.create(oid, sink.size())
stream = pa.FixedSizeBufferWriter(buf)
stream_writer = pa.RecordBatchStreamWriter(stream, table.schema)
for batch in table.to_batches():
    stream_writer.write_batch(batch)
stream_writer.close()
client.seal(oid)

buf2, = client.get_buffers([oid])
reader = pa.RecordBatchStreamReader(buf2)
record_batch = reader.read_next_batch()
df2 = record_batch.to_pandas()
print(df2)