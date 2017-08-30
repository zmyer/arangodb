////////////////////////////////////////////////////////////////////////////////
/// @brief mock object for StorageEngine class.
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
#ifndef ARANGOD_TRANSACTION_TRANSACTIONREGISTRY_MOCK_H
#define ARANGOD_TRANSACTION_TRANSACTIONREGISTRY_MOCK_H 1

#include "RestServer/TransactionRegistryFeature.h"
#include "Transaction/TransactionRegistry.h"

using namespace arangodb;
using namespace arangodb::basics;

namespace arangodb {
namespace tests {

// MockTransactionRegistry is typically a stack object that appears early
//  in a SECTION.
class MockTransactionRegistry: public transaction::TransactionRegistry {
 public:
  // Constructor saves current global and destructor restores.
  //  This ASSUMES only one MockTransactionRegistry object, or that multiple
  //  destruct in reverse order of construction.
  MockTransactionRegistry()
    : throw_getInfo_(false), mock_registry_id_(0)
  {
    old_global_ = TransactionRegistryFeature::TRANSACTION_REGISTRY;
    TransactionRegistryFeature::TRANSACTION_REGISTRY = this;
  }

  ~MockTransactionRegistry() {
    TransactionRegistryFeature::TRANSACTION_REGISTRY = old_global_;
  }

  /// @brief get information on specific transaction
  ///        throws std::out_of_range exception
  TransactionInfo* getInfo (transaction::TransactionId const& tx_id,
                            TRI_vocbase_t* vocbase = nullptr) const override
  {
    if (throw_getInfo_) {
      THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given id not found");
    } // if

    return (TransactionInfo *)&mock_trans_info_;
  }

  /// @brief this coordinators registry Id
  uint64_t id() const;

  /// @brief give access to generator's registryId for coordinator validation
  uint64_t registryId() const override {return mock_registry_id_;};

  bool throw_getInfo_;
  uint64_t mock_registry_id_;
  transaction::TransactionRegistry::TransactionInfo mock_trans_info_;

 protected:
  transaction::TransactionRegistry * old_global_;

};

}} // two namespaces
#endif // ARANGOD_VOCBASE_METHODS_TRANSACTIONS_MOCK_H
