////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_VOCBASE_METHODS_TRANSACTIONS_HANDLER_H
#define ARANGOD_VOCBASE_METHODS_TRANSACTIONS_HANDLER_H 1

#include "Basics/ReadWriteLock.h"
#include "Basics/Result.h"
#include "VocBase/vocbase.h"
#include <v8.h>
#include <velocypack/Builder.h>
#include <velocypack/velocypack-aliases.h>

namespace arangodb {

// this typedef is in support of unit test mocking
typedef std::tuple<Result, std::string> executeTransaction_t(
    v8::Isolate* isolate,
    basics::ReadWriteLock& lock,
    std::atomic<bool>& canceled,
    VPackSlice slice,
    std::string portType,
    VPackBuilder& builder,
    bool expectAction
  );

/// @brief RestHandler interface to single transaction and start transactin
executeTransaction_t executeTransaction;

/// @brief AQL interface to single transaction
std::tuple<Result, std::string> executeTransactionJS(
  v8::Isolate*, v8::Handle<v8::Value> const& arg,
  v8::Handle<v8::Value>& result, v8::TryCatch&, bool expectAction);

/// @brief Internal function exposed only for unit test purposes
bool getCollections(
  v8::Isolate* isolate,
  v8::Handle<v8::Object> obj,
  std::vector<std::string>& collections,
  char const* attributeName,
  std::string &collectionError);

}
#endif
