////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2017 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Daniel H. Larkin
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGO_ROCKSDB_ROCKSDB_COMPACTION_FILTER_H
#define ARANGO_ROCKSDB_ROCKSDB_COMPACTION_FILTER_H 1

#include "Basics/Common.h"

#include <rocksdb/compaction_filter.h>
#include <rocksdb/slice.h>

#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

namespace arangodb {
class RocksDBEngine;

class RocksDBCompactionFilter final : public rocksdb::CompactionFilter {
 public:
  virtual bool Filter(int level, const rocksdb::Slice& key,
                      const rocksdb::Slice& existing_value,
                      std::string* new_value,
                      bool* value_changed) const override;

  virtual const char* Name() const { return "ArangoCompactionFilter"; }
};

}  // namespace arangodb

#endif
