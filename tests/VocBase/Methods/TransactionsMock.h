////////////////////////////////////////////////////////////////////////////////
/// @brief mock object for Transaction.cpp
///
/// @file
///
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
/// @author Matthew Von-Maszewski
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_VOCBASE_METHODS_TRANSACTIONS_MOCK_H
#define ARANGOD_VOCBASE_METHODS_TRANSACTIONS_MOCK_H 1

#include "Transaction/Context.h"
#include "Transaction/Options.h"
#include "Transaction/TransactionRegistryMock.h"
#include "Transaction/UserTransaction.h"
#include "Transaction/V8Context.h"
#include "VocBase/Methods/Transactions.h"

using namespace arangodb;

namespace arangodb {
namespace tests {

// MockTransactions is typically a stack object that appears early
//  in a SECTION.
class MockTransactions {
 public:
  // Constructor saves current global and destructor restores.
  //  This ASSUMES only one MockTransactions object, or that multiple
  //  destruct in reverse order of construction.
  MockTransactions()
  {
    old_global_ = RestTransactionHandler::_executeTransactionPtr;
    RestTransactionHandler::_executeTransactionPtr = & executeTransactionMock;

    mock_txn_registry_.mock_registry_id_ = 54321;
  }

  ~MockTransactions() {
    RestTransactionHandler::_executeTransactionPtr = old_global_;
  }

  struct MOCK_vocbase_t : public TRI_vocbase_t {
    MOCK_vocbase_t() {};
  };


  static std::tuple<Result, std::string> executeTransactionMock(
    v8::Isolate* isolate,
    basics::ReadWriteLock& lock,
    std::atomic<bool>& canceled,
    VPackSlice slice,
    std::string portType,
    VPackBuilder& builder,
    bool expectAction) {

    Result result;
    std::string txnString = "54321-98765";

    return std::make_tuple(result, txnString);
  }

  MockTransactionRegistry mock_txn_registry_;

 protected:
  executeTransaction_t * old_global_;

};

}} // two namespaces
#endif  // #ifndef ARANGOD_VOCBASE_METHODS_TRANSACTIONS_MOCK_H
