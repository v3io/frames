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

#ifndef _ARROW_H_
#define _ARROW_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

extern const int BOOL_DTYPE;
extern const int FLOAT64_DTYPE;
extern const int INTEGER64_DTYPE;
extern const int STRING_DTYPE;
extern const int TIMESTAMP_DTYPE;

typedef struct {
  const char *err;
  union {
      void *ptr;
      char *cp;
      int64_t i;
      double f;
  };
} result_t;

// Allow access from Go
void *result_ptr(result_t r);
char *result_cp(result_t r);
int64_t result_i(result_t r);
double result_f(result_t r);

void *field_new(char *name, int type);
const char *field_name(void *field);
int field_dtype(void *vp);
void field_free(void *vp);

void *fields_new();
void fields_append(void *vp, void *fp);
void fields_free(void *vp);

void *schema_new(void *vp, void *metadata);
void schema_free(void *vp);
result_t schema_meta(void *vp);
result_t schema_set_meta(void *vp, void *mp);

result_t array_builder_new(int dtype);
result_t array_builder_append_bool(void *vp, int value);
result_t array_builder_append_float(void *vp, double value);
result_t array_builder_append_int(void *vp, int64_t value);
result_t array_builder_append_string(void *vp, char *value, size_t length);
result_t array_builder_append_timestamp(void *vp, int64_t value);
result_t array_builder_finish(void *vp);

int64_t array_length(void *vp);
void array_free(void *vp);

void *column_new(void *field, void *array);
void *column_field(void *vp);
int column_dtype(void *vp);
int64_t column_len(void *vp);
result_t column_bool_at(void *vp, long long i);
result_t column_int_at(void *vp, long long i);
result_t column_float_at(void *vp, long long i);
result_t column_string_at(void *vp, long long i);
result_t column_timestamp_at(void *vp, long long i);
result_t column_slice(void *vp, int64_t offset, int64_t length);
result_t column_copy_name(void *vp, const char *name);
void column_free(void *vp);

void *columns_new();
void columns_append(void *vp, void *cp);
void columns_free(void *vp);

void *table_new(void *sp, void *cp);
long long table_num_cols(void *vp);
long long table_num_rows(void *vp);
result_t table_col_by_index(void *vp, long long i);
result_t table_col_by_name(void *vp, const char *name);
result_t table_slice(void *vp, int64_t offset, int64_t length);
result_t table_schema(void *vp);
void table_free(void *vp);

void *meta_new();
result_t meta_set(void *vp, const char *key, const char *value); 
result_t meta_size(void *vp);
result_t meta_key(void *vp, int64_t i);
result_t meta_value(void *vp, int64_t i);

result_t plasma_connect(char *path);
result_t plasma_write(void *cp, void *tp, char *oid);
result_t plasma_read(void *cp, const char *oid, int64_t timeout_ms);
result_t plasma_release(void *cp, char *oid);
result_t plasma_disconnect(void *vp);

#ifdef __cplusplus
}
#endif // extern "C"

#endif // #ifdef _ARROW_H_
