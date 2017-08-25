////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
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
/// @author Jan Steemann
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_UTILS_SINGLE_COLLECTION_TRANSACTION_H
#define ARANGOD_UTILS_SINGLE_COLLECTION_TRANSACTION_H 1

#include "Basics/Common.h"
#include "Transaction/Methods.h"
#include "Utils/DatabaseGuard.h"
#include "VocBase/AccessMode.h"
#include "VocBase/voc-types.h"

namespace arangodb {
namespace transaction {
class Context;
class TransactionProxy;
}
namespace aql {
class Collection;
}

class Transaction final : public transaction::Methods {

  friend class transaction::TransactionProxy;

 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief create the transaction, using a collection id
  //////////////////////////////////////////////////////////////////////////////

  Transaction(std::shared_ptr<transaction::Context> const&,
              TRI_voc_cid_t, AccessMode::Type,
              transaction::Options const& options = transaction::Options());

  //////////////////////////////////////////////////////////////////////////////
  /// @brief create the transaction, using a collection name
  //////////////////////////////////////////////////////////////////////////////

  Transaction(std::shared_ptr<transaction::Context> const&,
              std::string const&, AccessMode::Type,
              transaction::Options const& options = transaction::Options());

  //////////////////////////////////////////////////////////////////////////////
  /// @brief create a transaction for AQL
  //////////////////////////////////////////////////////////////////////////////

  Transaction(std::shared_ptr<transaction::Context> const&, 
              std::map<std::string, aql::Collection*> const* collections,
              transaction::Options const& options,
              bool isMainTransaction);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief create the transaction, using lists of collection names and options
  //////////////////////////////////////////////////////////////////////////////

  Transaction(std::shared_ptr<transaction::Context> transactionContext,
              std::vector<std::string> const& readCollections,
              std::vector<std::string> const& writeCollections,
              std::vector<std::string> const& exclusiveCollections,
              transaction::Options const& options);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief create a transaction for replication
  //////////////////////////////////////////////////////////////////////////////
  
  Transaction(TRI_vocbase_t* vocbase);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief end the transaction
  //////////////////////////////////////////////////////////////////////////////

  ~Transaction() = default;

 private:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief get the underlying transaction collection
  //////////////////////////////////////////////////////////////////////////////

  TransactionCollection* trxCollection();

 public:

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get the underlying transaction collection
  //////////////////////////////////////////////////////////////////////////////
 
  TransactionCollection* trxCollection(TRI_voc_cid_t cid);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get the underlying document collection
  /// note that we have two identical versions because this is called
  /// in two different situations
  //////////////////////////////////////////////////////////////////////////////

  LogicalCollection* documentCollection();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get the underlying collection's id
  //////////////////////////////////////////////////////////////////////////////
  
  inline TRI_voc_cid_t cid() const { return _cid; }
  
  /// @brief add a collection to the transaction for read, at runtime
  TRI_voc_cid_t addCollectionAtRuntime(std::string const& collectionName) override final;
  /// @brief must counter name deletion to make the base class method available:
  TRI_voc_cid_t addCollectionAtRuntime(
      TRI_voc_cid_t cid,
      std::string const& collectionName,
      AccessMode::Type type = AccessMode::Type::READ) {
    return static_cast<transaction::Methods*>(this)->addCollectionAtRuntime(
        cid, collectionName, type);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get the underlying collection's name
  //////////////////////////////////////////////////////////////////////////////

  std::string name(TRI_voc_cid_t cid) const {
    // Just because of name deletion:
    return static_cast<transaction::Methods const*>(this)->name(cid);
  }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get the underlying collection's name
  //////////////////////////////////////////////////////////////////////////////

  std::string name();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief explicitly lock the underlying collection for read access
  //////////////////////////////////////////////////////////////////////////////

  Result lockRead();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief explicitly unlock the underlying collection after read access
  //////////////////////////////////////////////////////////////////////////////

  Result unlockRead();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief explicitly lock the underlying collection for write access
  //////////////////////////////////////////////////////////////////////////////

  Result lockWrite();
  
  /// @brief add a list of collections to the transaction, used for AQL only
  Result addCollections(
      std::map<std::string, aql::Collection*> const& collections) {
    Result res;
    for (auto const& it : collections) {
      res = processCollection(it.second);

      if (!res.ok()) {
        return res;
      }
    }
    return res;
  }

  /// @brief add a collection to the transaction
  Result processCollection(aql::Collection*); 

  /// @brief add a coordinator collection to the transaction
  Result processCollectionCoordinator(aql::Collection*);

  /// @brief add a regular collection to the transaction
  Result processCollectionNormal(aql::Collection* collection);

  /// @brief lockCollections, this is needed in a corner case in AQL: we need
  /// to lock all shards in a controlled way when we set up a distributed
  /// execution engine. To this end, we prevent the standard mechanism to
  /// lock collections on the DBservers when we instantiate the query. Then,
  /// in a second round, we need to lock the shards in exactly the right
  /// order via an HTTP call. This method is used to implement that HTTP action.
  int lockCollections() override;

 private:

  //////////////////////////////////////////////////////////////////////////////
  /// @brief collection id
  //////////////////////////////////////////////////////////////////////////////

  TRI_voc_cid_t _cid;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief trxCollection cache
  //////////////////////////////////////////////////////////////////////////////

  TransactionCollection* _trxCollection;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief LogicalCollection* cache
  //////////////////////////////////////////////////////////////////////////////

  LogicalCollection* _documentCollection;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief collection access type
  //////////////////////////////////////////////////////////////////////////////

  AccessMode::Type _accessType;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief protect the vocbase against deletion whilst we work
  //////////////////////////////////////////////////////////////////////////////

  DatabaseGuard _guard;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief note if we are a special transaction for replication
  //////////////////////////////////////////////////////////////////////////////
  
  bool _isForReplication;
};
}

#endif
