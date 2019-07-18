// +build arrow

/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <plasma/client.h>

#include <iostream>
#include <sstream>
#include <vector>

#include "arrow.h"
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

const int BOOL_DTYPE = arrow::Type::BOOL;
const int FLOAT64_DTYPE = arrow::Type::DOUBLE;
const int INTEGER64_DTYPE = arrow::Type::INT64;
const int STRING_DTYPE = arrow::Type::STRING;
const int TIMESTAMP_DTYPE = arrow::Type::TIMESTAMP;

static result_t new_result(const char *error=nullptr) {
    result_t r;
    r.err = error;
    r.ptr = nullptr;
    r.cp = nullptr;
    return r;
}

void *result_ptr(result_t r) { return r.ptr; }
char *result_cp(result_t r) { return r.cp; }
int64_t result_i(result_t r) { return r.i; }
double result_f(result_t r) { return r.f; }

#define RETURN_IF_ERROR(status)             \
  do {                                             \
    if (!status.ok()) {                            \
      return new_result(status.message().c_str()); \
    }                                              \
  } while (false)


static std::shared_ptr<arrow::DataType> data_type(int dtype) {
  switch (dtype) {
  case BOOL_DTYPE:
    return arrow::boolean();
  case FLOAT64_DTYPE:
    return arrow::float64();
  case INTEGER64_DTYPE:
    return arrow::int64();
  case STRING_DTYPE:
    return arrow::utf8();
  case TIMESTAMP_DTYPE:
    return arrow::timestamp(arrow::TimeUnit::NANO);
  }

  return nullptr;
}

void *field_new(char *name, int dtype) {
  auto dt = data_type(dtype);
  return new arrow::Field(name, dt);
}

const char *field_name(void *vp) {
  auto field = (arrow::Field *)vp;
  return field->name().c_str();
}

int field_dtype(void *vp) {
  auto field = (arrow::Field *)vp;
  return field->type()->id();
}

void field_free(void *vp) {
  if (vp == nullptr) {
    return;
  }
  auto field = (arrow::Field *)vp;
  delete field;
}

void *fields_new() { return new std::vector<std::shared_ptr<arrow::Field>>(); }

void fields_append(void *vp, void *fp) {
  auto fields = (std::vector<std::shared_ptr<arrow::Field>> *)vp;
  std::shared_ptr<arrow::Field> field((arrow::Field *)fp);
  fields->push_back(field);
}

void fields_free(void *vp) {
  if (vp == nullptr) {
    return;
  }
  delete (std::vector<std::shared_ptr<arrow::Field>> *)vp;
}

void *schema_new(void *vp) {
  auto fields = (std::vector<std::shared_ptr<arrow::Field>> *)vp;
  auto schema = new arrow::Schema(*fields);
  return (void *)schema;
}

void schema_free(void *vp) {
  if (vp == nullptr) {
    return;
  }
  auto schema = (arrow::Schema *)vp;
  delete schema;
}

result_t array_builder_new(int dtype) {
  auto res = new_result();
  switch (dtype) {
  case BOOL_DTYPE:
    res.ptr = new arrow::BooleanBuilder();
    break;
  case FLOAT64_DTYPE:
    res.ptr = new arrow::DoubleBuilder();
    break;
  case INTEGER64_DTYPE:
    res.ptr = new arrow::Int64Builder();
    break;
  case STRING_DTYPE:
    res.ptr = new arrow::StringBuilder();
    break;
  case TIMESTAMP_DTYPE:
    res.ptr = new arrow::TimestampBuilder(data_type(TIMESTAMP_DTYPE), nullptr);
    break;
  default:
    std::ostringstream oss;
    oss << "unknown dtype: " << dtype;
    res.err = oss.str().c_str();
  }

  return res;
}

// TODO: Check for nulls in all append
result_t array_builder_append_bool(void *vp, int value) {
  auto builder = (arrow::BooleanBuilder *)vp;
  auto status = builder->Append(bool(value));
  RETURN_IF_ERROR(status);
  return new_result();
}

result_t array_builder_append_float(void *vp, double value) {
  auto builder = (arrow::DoubleBuilder *)vp;
  auto status = builder->Append(value);
  RETURN_IF_ERROR(status);
  return new_result();
}

result_t array_builder_append_int(void *vp, int64_t value) {
  auto builder = (arrow::Int64Builder *)vp;
  auto status = builder->Append(value);
  RETURN_IF_ERROR(status);
  return new_result();
}

result_t array_builder_append_string(void *vp, char *cp, size_t length) {
  auto builder = (arrow::StringBuilder *)vp;
  auto status = builder->Append(cp, length);
  RETURN_IF_ERROR(status);
  return result_t{nullptr};
}

result_t array_builder_append_timestamp(void *vp, int64_t value) {
  auto builder = (arrow::TimestampBuilder *)vp;
  auto status = builder->Append(value);
  RETURN_IF_ERROR(status);
  return result_t{nullptr};
}

// TODO: See comment in struct Table
struct Array {
  std::shared_ptr<arrow::Array> array;
};

result_t array_builder_finish(void *vp) {
  auto builder = (arrow::ArrayBuilder *)vp;
  std::shared_ptr<arrow::Array> array;
  auto status = builder->Finish(&array);
  RETURN_IF_ERROR(status);
  delete builder;

  auto wrapper = new Array;
  wrapper->array = array;
  return result_t{nullptr, wrapper};
}

int64_t array_length(void *vp) {
  if (vp == nullptr) {
    return -1;
  }

  auto wrapper = (Array *)vp;
  return wrapper->array->length();
}

void array_free(void *vp) {
  if (vp == nullptr) {
    return;
  }

  delete (Array *)vp;
}

struct Column {
  std::shared_ptr<arrow::Column> ptr;
};

void *column_new(void *fp, void *ap) {
  std::shared_ptr<arrow::Field> field((arrow::Field *)fp);
  auto wrapper = (Array *)ap;
  auto ptr = std::make_shared<arrow::Column>(field, wrapper->array);
  auto col = new Column;
  col->ptr = ptr;
  return col;
}

int column_dtype(void *vp) {
  auto column = (Column *)vp;
  return column->ptr->type()->id();
}

int64_t column_len(void *vp) {
  auto column = (Column *)vp;
  if (column == nullptr) {
    return -1;
  }
  return column->ptr->length();
}

typedef struct {
	std::shared_ptr<arrow::Array> chunk;
	int64_t offset;
	const char *error;
} chunk_t;

chunk_t find_chunk(void *vp, long long i, int typ) {
	chunk_t ct = {nullptr, 0, nullptr};

  auto column = (Column *)vp;
	if (column == nullptr) {
		ct.error = "null pointer";
		return ct;
	}

  if (column->ptr->type()->id() != typ) {
		ct.error = "wrong type";
		return ct;
  }

  if ((i < 0) || (i >= column->ptr->length())) {
		ct.error = "index out of range";
		return ct;
  }

  auto chunks = column->ptr->data();
	ct.offset = i;

	for (int c = 0; c < chunks->num_chunks(); c++) {
		auto chunk = chunks->chunk(c);
		if (ct.offset < chunk->length()) {
			ct.chunk = chunk;
			return ct;
		}
		ct.offset -= chunk->length();
	}

	ct.error = "can't get here";
	return ct;
}	

result_t column_bool_at(void *vp, long long i) {
	auto res = new_result();
	auto cr = find_chunk(vp, i, BOOL_DTYPE);
	if (cr.error != nullptr) {
		res.err = cr.error;
		return res;
	}

	auto arr = (arrow::BooleanArray *)(cr.chunk.get());
  res.i = arr->Value(cr.offset);
	return res;
}

result_t column_int_at(void *vp, long long i) {
	auto res = new_result();
	auto cr = find_chunk(vp, i, INTEGER64_DTYPE);
	if (cr.error != nullptr) {
		res.err = cr.error;
		return res;
	}

	auto arr = (arrow::Int64Array *)(cr.chunk.get());
  res.i = arr->Value(cr.offset);
	return res;
}

result_t column_float_at(void *vp, long long i) {
	auto res = new_result();
	auto cr = find_chunk(vp, i, FLOAT64_DTYPE);
	if (cr.error != nullptr) {
		res.err = cr.error;
		return res;
	}

	auto arr = (arrow::DoubleArray *)(cr.chunk.get());
  res.f = arr->Value(cr.offset);
	return res;
}

result_t column_string_at(void *vp, long long i) {
	auto res = new_result();
	auto cr = find_chunk(vp, i, STRING_DTYPE);
	if (cr.error != nullptr) {
		res.err = cr.error;
		return res;
	}


	auto arr = (arrow::StringArray *)(cr.chunk.get());
	// TODO: Use arr->GetView or arr->GetValue to avoid another malloc
	auto str = arr->GetString(cr.offset);
	res.cp = strdup(str.c_str());
	return res;
}

result_t column_timestamp_at(void *vp, long long i) {
	auto res = new_result();
	auto cr = find_chunk(vp, i, TIMESTAMP_DTYPE);
	if (cr.error != nullptr) {
		res.err = cr.error;
		return res;
	}
	auto arr = (arrow::TimestampArray *)(cr.chunk.get());
	res.i = arr->Value(cr.offset);
	return res;
}

result_t column_slice(void *vp, int64_t offset, int64_t length) {
	auto res = new_result();
  auto column = (Column *)vp;
	if (column == nullptr) {
		res.err = strdup("null pointer");
		return res;
	}

	auto ptr = column->ptr->Slice(offset, length);
	if (ptr == nullptr) {
		res.err = strdup("can't slice");
		return res;
	}

  auto slice = new Column;
  slice->ptr = ptr;
	res.ptr = slice;
	return res;
}

void column_free(void *vp) {
  if (vp == nullptr) {
    return;
  }
  auto column = (Column *)vp;
  delete column;
}

void *column_field(void *vp) {
  auto column = (Column *)vp;
  return column->ptr->field().get();
}

void *columns_new() {
  return new std::vector<std::shared_ptr<arrow::Column>>();
}

void columns_append(void *vp, void *cp) {
  auto columns = (std::vector<std::shared_ptr<arrow::Column>> *)vp;
  auto column = (Column *)cp;
  columns->push_back(column->ptr);
}

void columns_free(void *vp) {
  auto columns = (std::vector<std::shared_ptr<arrow::Column>> *)vp;
  delete columns;
}

/* TODO: Do it with template (currently not possible under extern "C")
so we can unite with Array

e.g.
template <class T>
struct Shared<T> {
  std::shared_ptr<T> ptr;
};
*/

struct Table {
  std::shared_ptr<arrow::Table> ptr;
};

void *table_new(void *sp, void *cp) {
  std::shared_ptr<arrow::Schema> schema((arrow::Schema *)sp);
  auto columns = (std::vector<std::shared_ptr<arrow::Column>> *)cp;

  auto ptr = arrow::Table::Make(schema, *columns);
  if (ptr == nullptr) {
    return nullptr;
  }

  auto table = new Table;
  table->ptr = ptr;
  return table;
}

long long table_num_cols(void *vp) {
  auto table = (Table *)vp;
  return table->ptr->num_columns();
}

result_t table_col_by_name(void *vp, const char *name) {
  if (vp == nullptr) {
    return result_t{"null pointer", nullptr};
  }
  auto table = (Table *)vp;

  auto ptr = table->ptr->GetColumnByName(name);
  if (ptr == nullptr) {
    return result_t{"not found", nullptr};
  }

  auto column = new Column;
  column->ptr = ptr;

  return result_t{nullptr, column};
}

result_t table_col_by_index(void *vp, long long i) {
  if (vp == nullptr) {
    return result_t{"null pointer", nullptr};
  }

  auto table = (Table *)vp;
  auto ncols = table->ptr->num_columns();
  if ((i < 0) || (i >= ncols)) {
    std::ostringstream oss;
    oss << "column index " << i << "not in range [0:" << ncols << "]";
    return result_t{oss.str().c_str(), nullptr};
  }

  auto column = new Column;
  column->ptr = table->ptr->column(i);
  return result_t{nullptr, column};
}

long long table_num_rows(void *vp) {
  auto table = (Table *)vp;
  return table->ptr->num_rows();
}

result_t table_slice(void *vp, int64_t offset, int64_t length) {
	auto res = new_result();
  auto table = (Table *)vp;
	if (table == nullptr) {
		res.err = strdup("null pointer");
		return res;
	}

	auto ptr = table->ptr->Slice(offset, length);
	if (ptr == nullptr) {
		res.err = strdup("can't slice");
		return res;
	}

  auto slice = new Table;
  slice->ptr = ptr;
	res.ptr = slice;
	return res;
}

void table_free(void *vp) {
  if (vp == nullptr) {
    return;
  }

  delete (Table *)vp;
}

result_t plasma_connect(char *path) {
  plasma::PlasmaClient *client = new plasma::PlasmaClient();
  auto status = client->Connect(path, "", 0);
  if (!status.ok()) {
    client->Disconnect();
    delete client;
  }

  RETURN_IF_ERROR(status);
  return result_t{nullptr, client};
}

static arrow::Status
write_table(std::shared_ptr<arrow::Table> table,
            std::shared_ptr<arrow::ipc::RecordBatchWriter> writer) {
  arrow::TableBatchReader rdr(*table);

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = rdr.ReadNext(&batch);
    if (!status.ok()) {
      return status;
    }

    if (batch == nullptr) {
      break;
    }

    status = writer->WriteRecordBatch(*batch, true);
    if (!status.ok()) {
      return status;
    }
  }

  return arrow::Status::OK();
}

result_t table_size(std::shared_ptr<arrow::Table> table) {
  arrow::TableBatchReader rdr(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::io::MockOutputStream stream;

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(
      &stream, table->schema(), &writer);
  RETURN_IF_ERROR(status);
  status = write_table(table, writer);
  RETURN_IF_ERROR(status);
  status = writer->Close();
  RETURN_IF_ERROR(status);

  auto num_written = stream.GetExtentBytesWritten();
  return result_t{nullptr, (void *)num_written};
}

result_t plasma_write(void *cp, void *tp, char *oid) {
  if ((cp == nullptr) || (tp == nullptr) || (oid == nullptr)) {
    return result_t{"null pointer", nullptr};
  }

  auto client = (plasma::PlasmaClient *)(cp);
  auto table = (Table *)(tp);
  auto res = table_size(table->ptr);
  if (res.err != nullptr) {
    return res;
  }

  auto size = int64_t(res.ptr);

  plasma::ObjectID id = plasma::ObjectID::from_binary(oid);
  std::shared_ptr<arrow::Buffer> buf;
  // TODO: Check padding
  auto status = client->Create(id, size, nullptr, 0, &buf);
  RETURN_IF_ERROR(status);

  arrow::io::FixedSizeBufferWriter bw(buf);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  status =
      arrow::ipc::RecordBatchStreamWriter::Open(&bw, table->ptr->schema(), &writer);
  RETURN_IF_ERROR(status);

  status = write_table(table->ptr, writer);
  RETURN_IF_ERROR(status);
  status = client->Seal(id);
  RETURN_IF_ERROR(status);

  return result_t{nullptr, (void *)size};
}

result_t plasma_disconnect(void *vp) {
  if (vp == nullptr) {
    return result_t{nullptr, nullptr};
  }

  auto client = (plasma::PlasmaClient *)(vp);
  auto status = client->Disconnect();
  RETURN_IF_ERROR(status);
  delete client;
  return result_t{nullptr, nullptr};
}

// TODO: Do we want allowing multiple IDs? (like the client Get)
result_t plasma_read(void *cp, char *oid, int64_t timeout_ms) {
  if ((cp == nullptr) || (oid == nullptr)) {
    return result_t{"null pointer", nullptr};
  }

  auto client = (plasma::PlasmaClient *)(cp);

  plasma::ObjectID id = plasma::ObjectID::from_binary(oid);
  std::vector<plasma::ObjectID> ids;
  ids.push_back(id);
  std::vector<plasma::ObjectBuffer> buffers;

  auto status = client->Get(ids, timeout_ms, &buffers);
  RETURN_IF_ERROR(status);

  // TODO: Support multiple buffers
  if (buffers.size() != 1) {
    std::ostringstream oss;
    oss << "more than one buffer for " << oid;
    return result_t{oss.str().c_str(), nullptr};
  }

  auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffers[0].data);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  status = arrow::ipc::RecordBatchStreamReader::Open(buf_reader, &reader);
  RETURN_IF_ERROR(status);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    status = reader->ReadNext(&batch);
    RETURN_IF_ERROR(status);
    if (batch == nullptr) {
      break;
    }
    batches.push_back(batch);
  }

  std::shared_ptr<arrow::Table> table;
  status = arrow::Table::FromRecordBatches(batches, &table);
  RETURN_IF_ERROR(status);

  auto wrapper = new Table;
  wrapper->ptr = table;
  return result_t{nullptr, wrapper};
}

result_t plasma_release(void *cp, char *oid) {
  if ((cp == nullptr) || (oid == nullptr)) {
    return result_t{"null pointer", nullptr};
  }

  auto client = (plasma::PlasmaClient *)(cp);
  plasma::ObjectID id = plasma::ObjectID::from_binary(oid);
  auto status = client->Release(id);
  RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}

#ifdef __cplusplus
} // extern "C"
#endif
