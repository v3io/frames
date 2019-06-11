// +build carrow

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <plasma/client.h>

#include <iostream>
#include <vector>
#include <sstream>

#include "carrow.h"

#ifdef __cplusplus
extern "C" {
#endif

const int BOOL_DTYPE = arrow::Type::BOOL;
const int FLOAT64_DTYPE = arrow::Type::DOUBLE;
const int INTEGER64_DTYPE = arrow::Type::INT64;
const int STRING_DTYPE = arrow::Type::STRING;
const int TIMESTAMP_DTYPE = arrow::Type::TIMESTAMP;

/* TODO: Remove these */
void warn(arrow::Status status) {
  if (status.ok()) {
    return;
  }
  std::cout << "CARROW:WARNING: " << status.message() << "\n";
}

void debug_mark(std::string msg = "HERE") {
  std::cout << "\033[1;31m";
  std::cout << "<< " <<  msg << " >>\n";
  std::cout << "\033[0m";
  std::cout.flush();
}

#define CARROW_RETURN_IF_ERROR(status) \
  do { \
    if (!status.ok()) { \
      return result_t{status.message().c_str(), nullptr}; \
    } \
  } while (false)

std::shared_ptr<arrow::DataType> data_type(int dtype) {
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
  result_t res = {nullptr, nullptr};
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
  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}

result_t array_builder_append_float(void *vp, double value) {
  auto builder = (arrow::DoubleBuilder *)vp;
  auto status = builder->Append(value);
  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}

result_t array_builder_append_int(void *vp, long long value) {
  auto builder = (arrow::Int64Builder *)vp;
  auto status = builder->Append(value);
  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}

result_t array_builder_append_string(void *vp, char *cp, size_t length) {
  auto builder = (arrow::StringBuilder *)vp;
  auto status = builder->Append(cp, length);
  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}

result_t array_builder_append_timestamp(void *vp, long long value) {
  auto builder = (arrow::TimestampBuilder *)vp;
  auto status = builder->Append(value);
  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}


// TODO: See comment in struct Table
struct Array {
  std::shared_ptr<arrow::Array> array;
};

result_t array_builder_finish(void *vp) {
  auto builder = (arrow::ArrayBuilder *)vp;
  std::shared_ptr<arrow::Array> array;
  auto status = builder->Finish(&array);
  CARROW_RETURN_IF_ERROR(status);
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

void *column_new(void *fp, void *ap) {
  std::shared_ptr<arrow::Field> field((arrow::Field *)fp);
  auto wrapper = (Array *)ap;

  return new arrow::Column(field, wrapper->array);
}

int column_dtype(void *vp) {
  auto column = (arrow::Column *)vp;
  return column->type()->id();
}

void column_free(void *vp) {
  if (vp == nullptr) {
    return;
  }
  auto column = (arrow::Column *)vp;
  delete column;
}

void *column_field(void *vp) {
  auto column = (arrow::Column *)vp;
  return column->field().get();
}

void *columns_new() {
  return new std::vector<std::shared_ptr<arrow::Column>>();
}

void columns_append(void *vp, void *cp) {
  auto columns = (std::vector<std::shared_ptr<arrow::Column>> *)vp;
  std::shared_ptr<arrow::Column> column((arrow::Column *)cp);
  columns->push_back(column);
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
  std::shared_ptr<arrow::Table> table;
};

void *table_new(void *sp, void *cp) {
  std::shared_ptr<arrow::Schema> schema((arrow::Schema *)sp);
  auto columns = (std::vector<std::shared_ptr<arrow::Column>> *)cp;

  auto table = arrow::Table::Make(schema, *columns);
  if (table == nullptr) {
    return nullptr;
  }

  auto wrapper = new Table;
  wrapper->table = table;
  return wrapper;
}

long long table_num_cols(void *vp) {
  auto wrapper = (Table *)vp;
  return wrapper->table->num_columns();
}

long long table_num_rows(void *vp) {
  auto wrapper = (Table *)vp;
  return wrapper->table->num_rows();
}

void table_free(void *vp) {
  if (vp == nullptr) {
    return;
  }

  delete (Table *)vp;
}

result_t plasma_connect(char *path) {
  plasma::PlasmaClient* client = new plasma::PlasmaClient();
  auto status = client->Connect(path, "", 0);
  if (!status.ok()) {
    client->Disconnect();
    delete client;
  }

  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, client};
}

arrow::Status write_table(std::shared_ptr<arrow::Table> table, std::shared_ptr<arrow::ipc::RecordBatchWriter> writer) {
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
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(&stream, table->schema(), &writer);
  CARROW_RETURN_IF_ERROR(status);
  status = write_table(table, writer);
  CARROW_RETURN_IF_ERROR(status);
  status = writer->Close();
  CARROW_RETURN_IF_ERROR(status);

  auto num_written = stream.GetExtentBytesWritten();
  return result_t{nullptr, (void*)num_written};
}

result_t plasma_write(void *cp, void *tp, char *oid) {
  if ((cp == nullptr) || (tp == nullptr) || (oid == nullptr)) {
    return result_t{"null pointer", nullptr};
  }

  auto client = (plasma::PlasmaClient *)(cp);
  auto ptr = (Table *)(tp);
  auto table = ptr->table;

  auto res = table_size(table);
  if (res.err != nullptr) {
    return res;
  }

  auto size = int64_t(res.ptr);

  plasma::ObjectID id = plasma::ObjectID::from_binary(oid);
  std::shared_ptr<arrow::Buffer> buf;
  // TODO: Check padding
  auto status = client->Create(id, size, nullptr, 0, &buf);
  CARROW_RETURN_IF_ERROR(status);

  arrow::io::FixedSizeBufferWriter bw(buf);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  status = arrow::ipc::RecordBatchStreamWriter::Open(&bw, table->schema(), &writer);
  CARROW_RETURN_IF_ERROR(status);

  status = write_table(table, writer);
  CARROW_RETURN_IF_ERROR(status);
  status = client->Seal(id);
  CARROW_RETURN_IF_ERROR(status);

  return result_t{nullptr, (void *)size};
}

result_t plasma_disconnect(void *vp) {
  if (vp == nullptr) {
    return result_t{nullptr, nullptr};
  }

  auto client = (plasma::PlasmaClient*)(vp);
  auto status = client->Disconnect();
  CARROW_RETURN_IF_ERROR(status);
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
  CARROW_RETURN_IF_ERROR(status);

  // TODO: Support multiple buffers
  if (buffers.size() != 1) {
    std::ostringstream oss;
    oss << "more than one buffer for " << oid;
    return result_t{oss.str().c_str(), nullptr};
  }

  auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffers[0].data);
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  status = arrow::ipc::RecordBatchStreamReader::Open(buf_reader, &reader);
  CARROW_RETURN_IF_ERROR(status);

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (true)
  {
    std::shared_ptr<arrow::RecordBatch> batch;
    status = reader->ReadNext(&batch);
    CARROW_RETURN_IF_ERROR(status);
    if (batch == nullptr) {
      break;
    }
    batches.push_back(batch);
  }

  std::shared_ptr<arrow::Table> table;
  status = arrow::Table::FromRecordBatches(batches, &table);
  CARROW_RETURN_IF_ERROR(status);

  auto ptr = new Table;
  ptr->table = table;
  return result_t{nullptr, ptr};
}

result_t plasma_release(void *cp, char *oid) {
  if ((cp == nullptr) || (oid == nullptr)) {
    return result_t{"null pointer", nullptr};
  }

  auto client = (plasma::PlasmaClient *)(cp);
  plasma::ObjectID id = plasma::ObjectID::from_binary(oid);
  auto status = client->Release(id);
  CARROW_RETURN_IF_ERROR(status);
  return result_t{nullptr, nullptr};
}

#ifdef __cplusplus
} // extern "C"
#endif
