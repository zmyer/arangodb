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

#include "RocksDBCompactionFilter.h"
#include "Basics/VelocyPackHelper.h"
#include "Logger/Logger.h"
#include "RocksDBEngine/RocksDBCommon.h"
#include "RocksDBEngine/RocksDBEngine.h"
#include "RocksDBEngine/RocksDBKey.h"
#include "RocksDBEngine/RocksDBPrefixExtractor.h"
#include "RocksDBEngine/RocksDBTypes.h"

#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::rocksutils;

bool RocksDBCompactionFilter::Filter(int level, const rocksdb::Slice& key,
                                     const rocksdb::Slice& existing_value,
                                     std::string* new_value,
                                     bool* value_changed) const {
  switch (static_cast<RocksDBEntryType>(key[0])) {
    case RocksDBEntryType::Document:
    case RocksDBEntryType::IndexValue:
    case RocksDBEntryType::UniqueIndexValue:
    case RocksDBEntryType::PrimaryIndexValue:
    case RocksDBEntryType::EdgeIndexValue:
    case RocksDBEntryType::FulltextIndexValue:
    case RocksDBEntryType::GeoIndexValue: {
      return globalRocksEngine()->hasIdBeenDropped(RocksDBKey::objectId(key));
    }
    default: { return false; }
  }
}
