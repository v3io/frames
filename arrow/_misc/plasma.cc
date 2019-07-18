#include <plasma/client.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <vector>

std::shared_ptr<arrow::Table> build_table() {
  arrow::Int64Builder builder;
  for (int64_t i = 0; i < 10; i++) {
    builder.Append(i);
  }
  std::shared_ptr<arrow::Array> array;
  auto status = builder.Finish(&array);
  if (!status.ok()) {
      std::cerr << "error: can't create array" << status.message() << "\n";
      return nullptr;
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.push_back(array);

  std::shared_ptr<arrow::Field> field(new arrow::Field("i", arrow::int64()));

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.push_back(field);
  std::shared_ptr<arrow::Schema> schema(new arrow::Schema(fields));

  return arrow::Table::Make(schema, arrays);
}

bool write_table(arrow::Table *table, std::shared_ptr<arrow::ipc::RecordBatchWriter> wtr) {
  arrow::TableBatchReader rdr(*table);
  std::shared_ptr<arrow::RecordBatch> batch;

  while (true) {
    auto status = rdr.ReadNext(&batch);
    if (!status.ok()) {
      return false;
    }

    if (batch == nullptr) {
      break;
    }

    status = wtr->WriteRecordBatch(*batch, true);
    if (!status.ok()) {
      return false;
    }
  }

  return true;
}


int64_t table_size(arrow::Table *table) {
  arrow::TableBatchReader rdr(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::io::MockOutputStream stream;

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(&stream, table->schema(), &writer);
  if (!status.ok()) {
    return -1;
  }

  if (!write_table(table, writer)) {
    return -1;
  }

  status = writer->Close();
  if (!status.ok()) {
    return -1;
  }

  return stream.GetExtentBytesWritten();
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "error: wrong number of arguments\n";
    std::exit(1);
  }

  auto olen = strlen(argv[2]);
  char oid[] = "00000000000000000000";
  memcpy(oid+20-olen, argv[2], olen);
  std::cout << "oid: " << oid << "\n";

  // Start up and connect a Plasma client.
  plasma::PlasmaClient client;
  auto status = client.Connect(argv[1], "");
  if (!status.ok()) {
      std::cerr << "error: can't connect" << status.message() << "\n";
      std::exit(1);
  }

  auto table = build_table();
  if (table == nullptr) {
    std::cerr << "error: build\n";
    std::exit(1);
  }

  auto size = table_size(table.get());
  if (size == -1) {
    std::exit(1);
  }

  std::cout << "table size " << size << "\n";

  plasma::ObjectID id = plasma::ObjectID::from_binary(oid);
  std::shared_ptr<arrow::Buffer> buf;
  status = client.Create(id, size, nullptr, 0, &buf);
  if (!status.ok()) {
    std::cerr << "error: create obj: " << status.message() << "\n";
    std::exit(1);
  }

  arrow::io::FixedSizeBufferWriter wb(buf);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> wtr;
  status = arrow::ipc::RecordBatchStreamWriter::Open(&wb, table->schema(), &wtr);
  if (!status.ok()) {
    std::cerr << "error: create writer: " << status.message() << "\n";
    std::exit(1);
  }
  if (!write_table(table.get(), wtr)) {
    std::cerr << "error: can't write table\n";
    std::exit(1);
  }
  status = wtr->Close();
  if (!status.ok()) {
    std::cerr << "error: close: " << status.message() << "\n";
    std::exit(1);
  }

  status = client.Seal(id);
  if (!status.ok()) {
    std::cerr << "error: seal: " << status.message() << "\n";
    std::exit(1);
  }

  std::cout << "OK\n";
}