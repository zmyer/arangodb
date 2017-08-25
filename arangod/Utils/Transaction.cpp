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

#include "Aql/Collection.h"
#include "StorageEngine/TransactionCollection.h"
#include "StorageEngine/TransactionState.h"
#include "Utils/CollectionNameResolver.h"
#include "Utils/OperationResult.h"
#include "Utils/Transaction.h"
#include "Transaction/Context.h"
#include "Transaction/StandaloneContext.h"
#include "Transaction/Methods.h"
#include "VocBase/LogicalCollection.h"

using namespace arangodb;

/// @brief create the transaction, using a collection id
Transaction::Transaction(
  std::shared_ptr<transaction::Context> const& transactionContext,
  TRI_voc_cid_t cid, 
  AccessMode::Type accessType,
  transaction::Options const& options)
      : transaction::Methods(transactionContext, options),
        _cid(cid),
        _trxCollection(nullptr),
        _documentCollection(nullptr),
        _accessType(accessType),
        _guard(transactionContext->vocbase()),
        _isForReplication(false) {

  // add the (sole) collection
  addCollection(cid, _accessType);
  addHint(transaction::Hints::Hint::NO_DLD);
}

/// @brief create the transaction, using a collection name
Transaction::Transaction(
  std::shared_ptr<transaction::Context> const& transactionContext,
  std::string const& name, AccessMode::Type accessType,
  transaction::Options const& options)
      : transaction::Methods(transactionContext, options),
        _cid(0),
        _trxCollection(nullptr),
        _documentCollection(nullptr),
        _accessType(accessType),
        _guard(transactionContext->vocbase()),
        _isForReplication(false) {
  // add the (sole) collection
  _cid = resolver()->getCollectionId(name);
  addCollection(_cid, name.c_str(), _accessType);
  addHint(transaction::Hints::Hint::NO_DLD);
}

Transaction::Transaction(
    std::shared_ptr<transaction::Context> const& transactionContext, 
    std::map<std::string, aql::Collection*> const* collections,
    transaction::Options const& options,
    bool isMainTransaction)
  : transaction::Methods(transactionContext, options),
    _cid(0),
    _trxCollection(nullptr),
    _documentCollection(nullptr),
    _accessType(AccessMode::Type::NONE),
    _guard(transactionContext->vocbase()),
    _isForReplication(false) {
  if (!isMainTransaction) {
    addHint(transaction::Hints::Hint::LOCK_NEVER);
  } else {
    addHint(transaction::Hints::Hint::LOCK_ENTIRELY);
  }

  for (auto it : *collections) {
    if (!processCollection(it.second).ok()) {
      break;
    }
  }
}

Transaction::Transaction(
    std::shared_ptr<transaction::Context> transactionContext,
    std::vector<std::string> const& readCollections,
    std::vector<std::string> const& writeCollections,
    std::vector<std::string> const& exclusiveCollections,
    transaction::Options const& options)
  : transaction::Methods(transactionContext, options),
    _cid(0),
    _trxCollection(nullptr),
    _documentCollection(nullptr),
    _accessType(AccessMode::Type::NONE),
    _guard(transactionContext->vocbase()),
    _isForReplication(false) {

  addHint(transaction::Hints::Hint::LOCK_ENTIRELY);

  for (auto const& it : exclusiveCollections) {
    addCollection(it, AccessMode::Type::EXCLUSIVE);
  }

  for (auto const& it : writeCollections) {
    addCollection(it, AccessMode::Type::WRITE);
  }

  for (auto const& it : readCollections) {
    addCollection(it, AccessMode::Type::READ);
  }
}

Transaction::Transaction(TRI_vocbase_t* vocbase)
  : transaction::Methods(transaction::StandaloneContext::Create(vocbase)),
    _cid(0),
    _trxCollection(nullptr),
    _documentCollection(nullptr),
    _accessType(AccessMode::Type::NONE),
    _guard(vocbase),
    _isForReplication(true) {
}

/// @brief get the underlying transaction collection
TransactionCollection* Transaction::trxCollection() {
  TRI_ASSERT(_cid > 0);

  if (_trxCollection == nullptr) {
    _trxCollection = _state->collection(_cid, _accessType);

    if (_trxCollection != nullptr) {
      _documentCollection = _trxCollection->collection();
    }
  }

  TRI_ASSERT(_trxCollection != nullptr);
  return _trxCollection;
}

/// @brief get the underlying document collection
/// note that we have two identical versions because this is called
/// in two different situations
LogicalCollection* Transaction::documentCollection() {
  if (_documentCollection != nullptr) {
    return _documentCollection;
  }
 
  trxCollection(); 
  TRI_ASSERT(_documentCollection != nullptr);

  return _documentCollection;
}
  
/// @brief get the underlying collection's name
std::string Transaction::name() { 
  trxCollection(); // will ensure we have the _trxCollection object set
  TRI_ASSERT(_trxCollection != nullptr);
  return _trxCollection->collectionName();
}

/// @brief explicitly lock the underlying collection for read access
Result Transaction::lockRead() {
  return lock(trxCollection(), AccessMode::Type::READ);
}

/// @brief explicitly unlock the underlying collection after read access
Result Transaction::unlockRead() {
  return unlock(trxCollection(), AccessMode::Type::READ);
}

/// @brief explicitly lock the underlying collection for write access
Result Transaction::lockWrite() {
  return lock(trxCollection(), AccessMode::Type::WRITE);
}

/// @brief add a collection to the transaction
Result Transaction::processCollection(aql::Collection* collection) {
  if (ServerState::instance()->isCoordinator()) {
    return processCollectionCoordinator(collection);
  }
  return processCollectionNormal(collection);
}

/// @brief add a coordinator collection to the transaction

Result Transaction::processCollectionCoordinator(aql::Collection*  collection) {
  TRI_voc_cid_t cid = resolver()->getCollectionId(collection->getName());

  return addCollection(cid, collection->getName().c_str(),
                       collection->accessType);
}

/// @brief add a regular collection to the transaction

Result Transaction::processCollectionNormal(aql::Collection* collection) {
  TRI_voc_cid_t cid = 0;

  arangodb::LogicalCollection const* col =
      resolver()->getCollectionStruct(collection->getName());
  /*if (col == nullptr) {
    auto startTime = TRI_microtime();
    auto endTime = startTime + 60.0;
    do {
      usleep(10000);
      if (TRI_microtime() > endTime) {
        break;
      }
      col = this->resolver()->getCollectionStruct(collection->getName());
    } while (col == nullptr);
  }
  */
  if (col != nullptr) {
    cid = col->cid();
  }

  Result res = addCollection(cid, collection->getName(), collection->accessType);

  if (res.ok() && col != nullptr) {
    collection->setCollection(const_cast<arangodb::LogicalCollection*>(col));
  }

  return res;
}

/// @brief lockCollections, this is needed in a corner case in AQL: we need
/// to lock all shards in a controlled way when we set up a distributed
/// execution engine. To this end, we prevent the standard mechanism to
/// lock collections on the DBservers when we instantiate the query. Then,
/// in a second round, we need to lock the shards in exactly the right
/// order via an HTTP call. This method is used to implement that HTTP action.

int Transaction::lockCollections() {
  return state()->lockCollections();
}

//////////////////////////////////////////////////////////////////////////////
/// @brief get the underlying transaction collection
//////////////////////////////////////////////////////////////////////////////
 
TransactionCollection* Transaction::trxCollection(TRI_voc_cid_t cid) {
  if (_isForReplication) {
    TRI_ASSERT(cid > 0);
    Result result;

    TransactionCollection* trxCollection = _state->collection(cid, AccessMode::Type::WRITE);

    if (trxCollection == nullptr) {
      int res = _state->addCollection(cid, AccessMode::Type::EXCLUSIVE, 0, true);
      result.reset(res);
      if (result.ok()) {
        result = _state->ensureCollections();
      }
      if (!result.ok()) {
        return nullptr;
      }
      trxCollection = _state->collection(cid, AccessMode::Type::EXCLUSIVE);
    }
    return trxCollection;
  } else {
    TRI_ASSERT(_state != nullptr);
    TRI_ASSERT(_state->status() == transaction::Status::RUNNING);

    return _state->collection(cid, AccessMode::Type::READ);
  }
}

/// @brief add a collection to the transaction for read, at runtime
TRI_voc_cid_t Transaction::addCollectionAtRuntime(
    std::string const& collectionName) {
  // Optimization for the case that a default collection has been set:
  if (_cid != 0) {
    return _cid;
  }
  if (collectionName == _collectionCache.name && !collectionName.empty()) {
    return _collectionCache.cid;
  }

  auto cid = resolver()->getCollectionIdLocal(collectionName);

  if (cid == 0) {
    throwCollectionNotFound(collectionName.c_str());
  }
  addCollectionAtRuntime(cid, collectionName);
  _collectionCache.cid = cid;
  _collectionCache.name = collectionName;
  return cid;
}

