import pyarrow as pa
import pandas as pd
import pyarrow.plasma as plasma
import numpy as np

client = plasma.connect('/tmp/plasma.db', '', 3)


# Create a Pandas DataFrame
d = {'one' : pd.Series([1., 2., 3.], index=['a', 'b', 'c']),
     'two' : pd.Series([1., 2., 3., 4.], index=['a', 'b', 'c', 'd'])}
df = pd.DataFrame(d)

# Convert the Pandas DataFrame into a PyArrow RecordBatch
record_batch = pa.RecordBatch.from_pandas(df)

# Create the Plasma object from the PyArrow RecordBatch. Most of the work here
# is done to determine the size of buffer to request from the object store.
object_id = plasma.ObjectID(np.random.bytes(20))
mock_sink = pa.MockOutputStream()
stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
stream_writer.write_batch(record_batch)
stream_writer.close()
data_size = mock_sink.size()
buf = client.create(object_id, data_size)

stream = pa.FixedSizeBufferWriter(buf)
stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
stream_writer.write_batch(record_batch)
stream_writer.close()
client.seal(object_id)

data, = client.get_buffers([object_id])
buf = pa.BufferReader(data)
reader = pa.RecordBatchStreamReader(buf)
record_batch = reader.read_next_batch()
df2 = record_batch.to_pandas()
print(df2)
