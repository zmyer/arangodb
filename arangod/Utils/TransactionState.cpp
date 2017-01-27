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

#include "TransactionState.h"
#include "Aql/QueryCache.h"
#include "Logger/Logger.h"
#include "Basics/Exceptions.h"
#include "Basics/StaticStrings.h"
#include "MMFiles/MMFilesDatafileHelper.h"
#include "MMFiles/MMFilesDocumentOperation.h"
#include "MMFiles/MMFilesLogfileManager.h"
#include "MMFiles/MMFilesPersistentIndexFeature.h"
#include "Utils/Transaction.h"
#include "Utils/TransactionCollection.h"
#include "VocBase/LogicalCollection.h"
#include "VocBase/modes.h"
#include "VocBase/ticks.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>

#ifdef ARANGODB_ENABLE_MAINTAINER_MODE

#define LOG_TRX(trx, level)  \
  LOG(TRACE) << "trx #" << trx->_id << "." << level << " (" << StatusTransaction(trx->_status) << "): " 

#else

#define LOG_TRX(...) while (0) LOG(TRACE)

#endif

using namespace arangodb;

static bool IsWrite(AccessMode::Type type) {
  return (type == AccessMode::Type::WRITE || type == AccessMode::Type::EXCLUSIVE);
}
  
/// @brief returns whether the collection is currently locked
static inline bool IsLocked(TransactionCollection const* trxCollection) {
  return (trxCollection->_lockType != AccessMode::Type::NONE);
}

/// @brief return the logfile manager
static inline MMFilesLogfileManager* GetMMFilesLogfileManager() {
  return MMFilesLogfileManager::instance();
}

/// @brief whether or not a transaction is read-only
static inline bool IsReadOnlyTransaction(TransactionState const* trx) {
  return (trx->_type == AccessMode::Type::READ);
}

/// @brief whether or not a specific hint is set for the transaction
static inline bool HasHint(TransactionState const* trx,
                           TransactionHints::Hint hint) {
  return trx->_hints.has(hint);
}

/// @brief whether or not a transaction consists of a single operation
static inline bool IsSingleOperationTransaction(TransactionState const* trx) {
  return HasHint(trx, TransactionHints::Hint::SINGLE_OPERATION);
}

/// @brief whether or not a marker needs to be written
static inline bool NeedWriteMarker(TransactionState const* trx,
                                   bool isBeginMarker) {
  if (isBeginMarker) {
    return (!IsReadOnlyTransaction(trx) && !IsSingleOperationTransaction(trx));
  }

  return (trx->_nestingLevel == 0 && trx->_beginWritten &&
          !IsReadOnlyTransaction(trx) && !IsSingleOperationTransaction(trx));
}

/// @brief clear the query cache for all collections that were modified by
/// the transaction
void ClearQueryCache(TransactionState* trx) {
  if (trx->_collections.empty()) {
    return;
  }

  try {
    std::vector<std::string> collections;
    for (auto& trxCollection : trx->_collections) {
      if (!IsWrite(trxCollection->_accessType) ||
          trxCollection->_operations == nullptr ||
          trxCollection->_operations->empty()) {
        // we're only interested in collections that may have been modified
        continue;
      }

      collections.emplace_back(trxCollection->_collection->name());
    }

    if (!collections.empty()) {
      arangodb::aql::QueryCache::instance()->invalidate(trx->_vocbase,
                                                        collections);
    }
  } catch (...) {
    // in case something goes wrong, we have to remove all queries from the
    // cache
    arangodb::aql::QueryCache::instance()->invalidate(trx->_vocbase);
  }
}

/// @brief return the status of the transaction as a string
#ifdef ARANGODB_ENABLE_MAINTAINER_MODE
static char const* StatusTransaction(Transaction::Status status) {
  switch (status) {
    case Transaction::Status::UNDEFINED:
      return "undefined";
    case Transaction::Status::CREATED:
      return "created";
    case Transaction::Status::RUNNING:
      return "running";
    case Transaction::Status::COMMITTED:
      return "committed";
    case Transaction::Status::ABORTED:
      return "aborted";
  }

  TRI_ASSERT(false);
  return "unknown";
}
#endif

/// @brief free all operations for a transaction
static void FreeOperations(arangodb::Transaction* activeTrx, TransactionState* trx) {
  bool const mustRollback = (trx->_status == Transaction::Status::ABORTED);
  bool const isSingleOperation = IsSingleOperationTransaction(trx);
     
  TRI_ASSERT(activeTrx != nullptr);
   
  for (auto& trxCollection : trx->_collections) {
    if (trxCollection->_operations == nullptr) {
      continue;
    }

    arangodb::LogicalCollection* collection = trxCollection->_collection;

    if (mustRollback) {
      // revert all operations
      for (auto it = trxCollection->_operations->rbegin();
           it != trxCollection->_operations->rend(); ++it) {
        MMFilesDocumentOperation* op = (*it);

        try {
          op->revert(activeTrx);
        } catch (...) {
        }
        delete op;
      }
    } else {
      // no rollback. simply delete all operations
      for (auto it = trxCollection->_operations->rbegin();
           it != trxCollection->_operations->rend(); ++it) {
        delete (*it);
      }
    }

    if (mustRollback) {
      collection->setRevision(trxCollection->_originalRevision, true);
    } else if (!collection->isVolatile() && !isSingleOperation) {
      // only count logfileEntries if the collection is durable
      collection->increaseUncollectedLogfileEntries(trxCollection->_operations->size());
    }

    delete trxCollection->_operations;
    trxCollection->_operations = nullptr;
  }
}

/// @brief find a collection in the transaction's list of collections
static TransactionCollection* FindCollection(
    TransactionState const* trx, TRI_voc_cid_t cid,
    size_t* position) {

  size_t const n = trx->_collections.size();
  size_t i;

  for (i = 0; i < n; ++i) {
    auto trxCollection = trx->_collections.at(i);

    if (cid < trxCollection->_cid) {
      // collection not found
      break;
    }

    if (cid == trxCollection->_cid) {
      // found
      return trxCollection;
    }
    // next
  }

  if (position != nullptr) {
    // update the insert position if required
    *position = i;
  }

  return nullptr;
}


/// @brief release collection locks for a transaction
static int ReleaseCollections(TransactionState* trx, int nestingLevel) {
  TRI_ASSERT(nestingLevel == 0);
  if (HasHint(trx, TransactionHints::Hint::LOCK_NEVER) ||
      HasHint(trx, TransactionHints::Hint::NO_USAGE_LOCK)) {
    return TRI_ERROR_NO_ERROR;
  }

  // process collections in reverse order
  for (auto it = trx->_collections.rbegin(); it != trx->_collections.rend(); ++it) {
    TransactionCollection* trxCollection = (*it);

    // the top level transaction releases all collections
    if (trxCollection->_collection != nullptr) {
      // unuse collection, remove usage-lock
      LOG_TRX(trx, nestingLevel) << "unusing collection " << trxCollection->_cid;

      trx->_vocbase->releaseCollection(trxCollection->_collection);
      trxCollection->_collection = nullptr;
    }
  }

  return TRI_ERROR_NO_ERROR;
}

/// @brief write WAL begin marker
static int WriteBeginMarker(TransactionState* trx) {
  if (!NeedWriteMarker(trx, true)) {
    return TRI_ERROR_NO_ERROR;
  }

  if (HasHint(trx, TransactionHints::Hint::NO_BEGIN_MARKER)) {
    return TRI_ERROR_NO_ERROR;
  }

  TRI_IF_FAILURE("TransactionWriteBeginMarker") { return TRI_ERROR_DEBUG; }

  TRI_ASSERT(!trx->_beginWritten);

  int res;

  try {
    MMFilesTransactionMarker marker(TRI_DF_MARKER_VPACK_BEGIN_TRANSACTION, trx->_vocbase->id(), trx->_id);
    res = GetMMFilesLogfileManager()->allocateAndWrite(marker, false).errorCode;
    
    TRI_IF_FAILURE("TransactionWriteBeginMarkerThrow") { 
      throw std::bad_alloc();
    }

    if (res == TRI_ERROR_NO_ERROR) {
      trx->_beginWritten = true;
    } else {
      THROW_ARANGO_EXCEPTION(res);
    }
  } catch (arangodb::basics::Exception const& ex) {
    res = ex.code();
    LOG(WARN) << "could not save transaction begin marker in log: " << ex.what();
  } catch (std::exception const& ex) {
    res = TRI_ERROR_INTERNAL;
    LOG(WARN) << "could not save transaction begin marker in log: " << ex.what();
  } catch (...) {
    res = TRI_ERROR_INTERNAL;
    LOG(WARN) << "could not save transaction begin marker in log: unknown exception";
  }

  return res;
}

/// @brief write WAL abort marker
static int WriteAbortMarker(TransactionState* trx) {
  if (!NeedWriteMarker(trx, false)) {
    return TRI_ERROR_NO_ERROR;
  }

  if (HasHint(trx, TransactionHints::Hint::NO_ABORT_MARKER)) {
    return TRI_ERROR_NO_ERROR;
  }

  TRI_ASSERT(trx->_beginWritten);

  TRI_IF_FAILURE("TransactionWriteAbortMarker") { return TRI_ERROR_DEBUG; }

  int res;

  try {
    MMFilesTransactionMarker marker(TRI_DF_MARKER_VPACK_ABORT_TRANSACTION, trx->_vocbase->id(), trx->_id);
    res = GetMMFilesLogfileManager()->allocateAndWrite(marker, false).errorCode;
    
    TRI_IF_FAILURE("TransactionWriteAbortMarkerThrow") { 
      throw std::bad_alloc();
    }
  
    if (res != TRI_ERROR_NO_ERROR) {
      THROW_ARANGO_EXCEPTION(res);
    }
  } catch (arangodb::basics::Exception const& ex) {
    res = ex.code();
    LOG(WARN) << "could not save transaction abort marker in log: " << ex.what();
  } catch (std::exception const& ex) {
    res = TRI_ERROR_INTERNAL;
    LOG(WARN) << "could not save transaction abort marker in log: " << ex.what();
  } catch (...) {
    res = TRI_ERROR_INTERNAL;
    LOG(WARN) << "could not save transaction abort marker in log: unknown exception";
  }

  return res;
}

/// @brief write WAL commit marker
static int WriteCommitMarker(TransactionState* trx) {
  if (!NeedWriteMarker(trx, false)) {
    return TRI_ERROR_NO_ERROR;
  }

  TRI_IF_FAILURE("TransactionWriteCommitMarker") { return TRI_ERROR_DEBUG; }

  TRI_ASSERT(trx->_beginWritten);

  int res;

  try {
    MMFilesTransactionMarker marker(TRI_DF_MARKER_VPACK_COMMIT_TRANSACTION, trx->_vocbase->id(), trx->_id);
    res = GetMMFilesLogfileManager()->allocateAndWrite(marker, trx->_waitForSync).errorCode;
    
    TRI_IF_FAILURE("TransactionWriteCommitMarkerSegfault") { 
      TRI_SegfaultDebugging("crashing on commit");
    }

    TRI_IF_FAILURE("TransactionWriteCommitMarkerNoRocksSync") { return TRI_ERROR_NO_ERROR; }

    if (trx->_waitForSync) {
      // also sync RocksDB WAL
      RocksDBFeature::syncWal();
    }
    
    TRI_IF_FAILURE("TransactionWriteCommitMarkerThrow") { 
      throw std::bad_alloc();
    }
    
    if (res != TRI_ERROR_NO_ERROR) {
      THROW_ARANGO_EXCEPTION(res);
    }
  } catch (arangodb::basics::Exception const& ex) {
    res = ex.code();
    LOG(WARN) << "could not save transaction commit marker in log: " << ex.what();
  } catch (std::exception const& ex) {
    res = TRI_ERROR_INTERNAL;
    LOG(WARN) << "could not save transaction commit marker in log: " << ex.what();
  } catch (...) {
    res = TRI_ERROR_INTERNAL;
    LOG(WARN) << "could not save transaction commit marker in log: unknown exception";
  }

  return res;
}

/// @brief update the status of a transaction
static void UpdateTransactionStatus(TransactionState* const trx,
                                    Transaction::Status status) {
  TRI_ASSERT(trx->_status == Transaction::Status::CREATED ||
             trx->_status == Transaction::Status::RUNNING);

  if (trx->_status == Transaction::Status::CREATED) {
    TRI_ASSERT(status == Transaction::Status::RUNNING ||
               status == Transaction::Status::ABORTED);
  } else if (trx->_status == Transaction::Status::RUNNING) {
    TRI_ASSERT(status == Transaction::Status::COMMITTED ||
               status == Transaction::Status::ABORTED);
  }

  trx->_status = status;
}

/// @brief return the collection from a transaction
TransactionCollection* TransactionState::collection(TRI_voc_cid_t cid, AccessMode::Type accessType) {
  TRI_ASSERT(_status == Transaction::Status::CREATED ||
             _status == Transaction::Status::RUNNING);

  TransactionCollection* trxCollection =
      FindCollection(this, cid, nullptr);

  if (trxCollection == nullptr) {
    // not found
    return nullptr;
  }

  if (trxCollection->_collection == nullptr) {
    if (!HasHint(this, TransactionHints::Hint::LOCK_NEVER) ||
        !HasHint(this, TransactionHints::Hint::NO_USAGE_LOCK)) {
      // not opened. probably a mistake made by the caller
      return nullptr;
    }
    // ok
  }

  // check if access type matches
  if (IsWrite(accessType) && !IsWrite(trxCollection->_accessType)) {
    // type doesn't match. probably also a mistake by the caller
    return nullptr;
  }

  return trxCollection;
}

/// @brief add a collection to a transaction
int TransactionState::addCollection(TRI_voc_cid_t cid,
                                    AccessMode::Type accessType,
                                    int nestingLevel, bool force,
                                    bool allowImplicitCollections) {
  LOG_TRX(this, nestingLevel) << "adding collection " << cid;

  allowImplicitCollections &= _allowImplicit;

  // LOG(TRACE) << "cid: " << cid 
  //            << ", accessType: " << accessType 
  //            << ", nestingLevel: " << nestingLevel 
  //            << ", force: " << force 
  //            << ", allowImplicitCollections: " << allowImplicitCollections;
  
  // upgrade transaction type if required
  if (nestingLevel == 0) {
    if (!force) {
      TRI_ASSERT(_status == Transaction::Status::CREATED);
    }

    if (IsWrite(accessType) && !IsWrite(_type)) {
      // if one collection is written to, the whole transaction becomes a
      // write-transaction
      _type = AccessMode::Type::WRITE;
    }
  }

  // check if we already have got this collection in the _collections vector
  size_t position = 0;
  TransactionCollection* trxCollection =
      FindCollection(this, cid, &position);
  
  if (trxCollection != nullptr) {
    // collection is already contained in vector

    if (IsWrite(accessType) && !IsWrite(trxCollection->_accessType)) {
      if (nestingLevel > 0) {
        // trying to write access a collection that is only marked with
        // read-access
        return TRI_ERROR_TRANSACTION_UNREGISTERED_COLLECTION;
      }

      TRI_ASSERT(nestingLevel == 0);

      // upgrade collection type to write-access
      trxCollection->_accessType = accessType;
    }

    if (nestingLevel < trxCollection->_nestingLevel) {
      trxCollection->_nestingLevel = nestingLevel;
    }

    // all correct
    return TRI_ERROR_NO_ERROR;
  }

  // collection not found.

  if (nestingLevel > 0 && IsWrite(accessType)) {
    // trying to write access a collection in an embedded transaction
    return TRI_ERROR_TRANSACTION_UNREGISTERED_COLLECTION;
  }

  if (!IsWrite(accessType) && !allowImplicitCollections) {
    return TRI_ERROR_TRANSACTION_UNREGISTERED_COLLECTION;
  }
  
  // collection was not contained. now create and insert it
  TRI_ASSERT(trxCollection == nullptr);
  try {
    trxCollection = new TransactionCollection(this, cid, accessType, nestingLevel);
  } catch (...) {
    return TRI_ERROR_OUT_OF_MEMORY;
  }
  
  TRI_ASSERT(trxCollection != nullptr);

  // insert collection at the correct position
  try {
    _collections.insert(_collections.begin() + position, trxCollection);
  } catch (...) {
    delete trxCollection;

    return TRI_ERROR_OUT_OF_MEMORY;
  }

  return TRI_ERROR_NO_ERROR;
}

/// @brief make sure all declared collections are used & locked
int TransactionState::ensureCollections(int nestingLevel) {
  return useCollections(nestingLevel);
}


/// @brief add a WAL operation for a transaction collection
int arangodb::TRI_AddOperationTransaction(TransactionState* trx,
                                TRI_voc_rid_t revisionId,
                                MMFilesDocumentOperation& operation,
                                MMFilesWalMarker const* marker,
                                bool& waitForSync) {
  LogicalCollection* collection = operation.collection();
  bool const isSingleOperationTransaction = IsSingleOperationTransaction(trx);

  if (HasHint(trx, TransactionHints::Hint::RECOVERY)) {
    // turn off all waitForSync operations during recovery
    waitForSync = false;
  } else if (!waitForSync) {
    // upgrade the info for the transaction based on the collection's settings
    waitForSync |= collection->waitForSync();
  }

  if (waitForSync) {
    trx->_waitForSync = true;
  }

  TRI_IF_FAILURE("TransactionOperationNoSlot") { return TRI_ERROR_DEBUG; }

  TRI_IF_FAILURE("TransactionOperationNoSlotExcept") {
    THROW_ARANGO_EXCEPTION(TRI_ERROR_DEBUG);
  }

  if (!isSingleOperationTransaction && !trx->_beginWritten) {
    int res = WriteBeginMarker(trx);

    if (res != TRI_ERROR_NO_ERROR) {
      return res;
    }
  }

  TRI_voc_fid_t fid = 0;
  void const* position = nullptr;

  if (marker->fid() == 0) {
    // this is a "real" marker that must be written into the logfiles
    // just append it to the WAL:

    // we only need to set waitForSync to true here if waitForSync was requested
    // for the operation AND the operation is a standalone operation. In case the
    // operation belongs to a transaction, the transaction's commit marker will
    // be written with waitForSync, and we don't need to request a sync ourselves
    bool const localWaitForSync = (isSingleOperationTransaction && waitForSync);

    // never wait until our marker was synced, even when an operation was tagged
    // waitForSync=true. this is still safe because inside a transaction, the final
    // commit marker will be written with waitForSync=true then, and in a standalone
    // operation the transaction will wait until everything was synced before returning
    // to the caller
    bool const waitForTick = false;

    // we should wake up the synchronizer in case this is a single operation
    //
    bool const wakeUpSynchronizer = isSingleOperationTransaction;

    MMFilesWalSlotInfoCopy slotInfo =
        MMFilesLogfileManager::instance()->allocateAndWrite(
            trx->_vocbase->id(), collection->cid(), 
            marker, wakeUpSynchronizer,
            localWaitForSync, waitForTick);
    if (slotInfo.errorCode != TRI_ERROR_NO_ERROR) {
      // some error occurred
      return slotInfo.errorCode;
    }
    if (localWaitForSync) {
      // also sync RocksDB WAL
      RocksDBFeature::syncWal();
    }
    operation.setTick(slotInfo.tick);
    fid = slotInfo.logfileId;
    position = slotInfo.mem;
  } else {
    // this is an envelope marker that has been written to the logfiles before.
    // avoid writing it again!
    fid = marker->fid();
    position = static_cast<MMFilesMarkerEnvelope const*>(marker)->mem();
  }

  TRI_ASSERT(fid > 0);
  TRI_ASSERT(position != nullptr);

  if (operation.type() == TRI_VOC_DOCUMENT_OPERATION_INSERT ||
      operation.type() == TRI_VOC_DOCUMENT_OPERATION_UPDATE ||
      operation.type() == TRI_VOC_DOCUMENT_OPERATION_REPLACE) {
    // adjust the data position in the header
    uint8_t const* vpack = reinterpret_cast<uint8_t const*>(position) + MMFilesDatafileHelper::VPackOffset(TRI_DF_MARKER_VPACK_DOCUMENT);
    TRI_ASSERT(fid > 0);
    operation.setVPack(vpack);
    collection->updateRevision(revisionId, vpack, fid, true); // always in WAL
  }

  TRI_IF_FAILURE("TransactionOperationAfterAdjust") { return TRI_ERROR_DEBUG; }

  if (isSingleOperationTransaction) {
    // operation is directly executed
    if (trx->_rocksTransaction != nullptr) {
      auto status = trx->_rocksTransaction->Commit();

      if (!status.ok()) { 
        // TODO: what to do here?
      }
    }
    operation.handled();

    arangodb::aql::QueryCache::instance()->invalidate(
        trx->_vocbase, collection->name());

    collection->increaseUncollectedLogfileEntries(1);
  } else {
    // operation is buffered and might be rolled back
    TransactionCollection* trxCollection = trx->collection(collection->cid(), AccessMode::Type::WRITE);

    if (trxCollection->_operations == nullptr) {
      trxCollection->_operations = new std::vector<MMFilesDocumentOperation*>;
      trxCollection->_operations->reserve(16);
      trx->_hasOperations = true;
    } else {
      // reserve space for one more element so the push_back below does not fail
      size_t oldSize = trxCollection->_operations->size();
      if (oldSize + 1 >= trxCollection->_operations->capacity()) {
        // double the size
        trxCollection->_operations->reserve((oldSize + 1) * 2);
      }
    }
    
    TRI_IF_FAILURE("TransactionOperationPushBack") {
      // test what happens if reserve above failed
      THROW_ARANGO_EXCEPTION(TRI_ERROR_DEBUG); 
    }

    std::unique_ptr<MMFilesDocumentOperation> copy(operation.swap());
    
    // should not fail because we reserved enough room above 
    trxCollection->_operations->push_back(copy.get());
    copy->handled();
    copy.release();
  }
   
  collection->setRevision(revisionId, false);

  TRI_IF_FAILURE("TransactionOperationAtEnd") { return TRI_ERROR_DEBUG; }

  return TRI_ERROR_NO_ERROR;
}

/// @brief start a transaction
int arangodb::TRI_BeginTransaction(TransactionState* trx, TransactionHints hints,
                         int nestingLevel) {
  LOG_TRX(trx, nestingLevel) << "beginning " << (IsWrite(trx->_type) ? "write" : "read") << " transaction";

  if (nestingLevel == 0) {
    TRI_ASSERT(trx->_status == Transaction::Status::CREATED);

    auto logfileManager = MMFilesLogfileManager::instance();

    if (!HasHint(trx, TransactionHints::Hint::NO_THROTTLING) &&
        IsWrite(trx->_type) &&
        logfileManager->canBeThrottled()) {
      // write-throttling?
      static uint64_t const WaitTime = 50000;
      uint64_t const maxIterations =
          logfileManager->maxThrottleWait() / (WaitTime / 1000);
      uint64_t iterations = 0;

      while (logfileManager->isThrottled()) {
        if (++iterations == maxIterations) {
          return TRI_ERROR_ARANGO_WRITE_THROTTLE_TIMEOUT;
        }

        usleep(WaitTime);
      }
    }

    // set hints
    trx->_hints = hints;

    // get a new id
    trx->_id = TRI_NewTickServer();

    // register a protector
    int res = logfileManager->registerTransaction(trx->_id);

    if (res != TRI_ERROR_NO_ERROR) {
      return res;
    }
  
  } else {
    TRI_ASSERT(trx->_status == Transaction::Status::RUNNING);
  }

  int res = trx->useCollections(nestingLevel);

  if (res == TRI_ERROR_NO_ERROR) {
    // all valid
    if (nestingLevel == 0) {
      UpdateTransactionStatus(trx, Transaction::Status::RUNNING);

      // defer writing of the begin marker until necessary!
    }
  } else {
    // something is wrong
    if (nestingLevel == 0) {
      UpdateTransactionStatus(trx, Transaction::Status::ABORTED);
    }

    // free what we have got so far
    trx->unuseCollections(nestingLevel);
  }

  return res;
}

/// @brief commit a transaction
int arangodb::TRI_CommitTransaction(arangodb::Transaction* activeTrx, TransactionState* trx, int nestingLevel) {
  LOG_TRX(trx, nestingLevel) << "committing " << (IsWrite(trx->_type) ? "write" : "read") << " transaction";

  TRI_ASSERT(trx->_status == Transaction::Status::RUNNING);

  int res = TRI_ERROR_NO_ERROR;

  if (nestingLevel == 0) {
    if (trx->_rocksTransaction != nullptr) {
      auto status = trx->_rocksTransaction->Commit();

      if (!status.ok()) {
        res = TRI_ERROR_INTERNAL;
        TRI_AbortTransaction(activeTrx, trx, nestingLevel);
        return res;
      }
    }

    res = WriteCommitMarker(trx);

    if (res != TRI_ERROR_NO_ERROR) {
      // TODO: revert rocks transaction somehow
      TRI_AbortTransaction(activeTrx, trx, nestingLevel);

      // return original error
      return res;
    }

    UpdateTransactionStatus(trx, Transaction::Status::COMMITTED);

    // if a write query, clear the query cache for the participating collections
    if (IsWrite(trx->_type) &&
        !trx->_collections.empty() &&
        arangodb::aql::QueryCache::instance()->mayBeActive()) {
      ClearQueryCache(trx);
    }

    FreeOperations(activeTrx, trx);
  }

  trx->unuseCollections(nestingLevel);

  return res;
}

/// @brief abort and rollback a transaction
int arangodb::TRI_AbortTransaction(arangodb::Transaction* activeTrx, TransactionState* trx, int nestingLevel) {
  LOG_TRX(trx, nestingLevel) << "aborting " << (IsWrite(trx->_type) ? "write" : "read") << " transaction";

  TRI_ASSERT(trx->_status == Transaction::Status::RUNNING);

  int res = TRI_ERROR_NO_ERROR;

  if (nestingLevel == 0) {
    res = WriteAbortMarker(trx);

    UpdateTransactionStatus(trx, Transaction::Status::ABORTED);

    FreeOperations(activeTrx, trx);
  }

  trx->unuseCollections(nestingLevel);

  return res;
}

/// @brief whether or not a transaction consists of a single operation
bool arangodb::TRI_IsSingleOperationTransaction(TransactionState const* trx) {
  return HasHint(trx, TransactionHints::Hint::SINGLE_OPERATION);
}

/// @brief transaction type
TransactionState::TransactionState(TRI_vocbase_t* vocbase, double timeout, bool waitForSync)
    : _vocbase(vocbase), 
      _id(0), 
      _type(AccessMode::Type::READ),
      _status(Transaction::Status::CREATED),
      _arena(),
      _collections{_arena}, // assign arena to vector 
      _rocksTransaction(nullptr),
      _hints(),
      _nestingLevel(0), 
      _allowImplicit(true),
      _hasOperations(false), 
      _waitForSync(waitForSync),
      _beginWritten(false), 
      _timeout(Transaction::DefaultLockTimeout) {
  
  if (timeout > 0.0) {
    _timeout = timeout;
  } 
}

/// @brief free a transaction container
TransactionState::~TransactionState() {
  TRI_ASSERT(_status != Transaction::Status::RUNNING);

  delete _rocksTransaction;

  ReleaseCollections(this, 0);

  // free all collections
  for (auto it = _collections.rbegin(); it != _collections.rend(); ++it) {
    delete (*it);
  }
}

/// @brief use all participating collections of a transaction
int TransactionState::useCollections(int nestingLevel) {
  // process collections in forward order
  for (auto& trxCollection : _collections) {
    if (trxCollection->_nestingLevel != nestingLevel) {
      // only process our own collections
      continue;
    }

    if (trxCollection->_collection == nullptr) {
      // open the collection
      if (!HasHint(this, TransactionHints::Hint::LOCK_NEVER) &&
          !HasHint(this, TransactionHints::Hint::NO_USAGE_LOCK)) {
        // use and usage-lock
        TRI_vocbase_col_status_e status;
        LOG_TRX(this, nestingLevel) << "using collection " << trxCollection->_cid;
        trxCollection->_collection = _vocbase->useCollection(trxCollection->_cid, status);
      } else {
        // use without usage-lock (lock already set externally)
        trxCollection->_collection = _vocbase->lookupCollection(trxCollection->_cid);

        if (trxCollection->_collection == nullptr) {
          return TRI_ERROR_ARANGO_COLLECTION_NOT_FOUND;
        }
      }

      if (trxCollection->_collection == nullptr) {
        // something went wrong
        int res = TRI_errno();

        if (res == TRI_ERROR_NO_ERROR) {
          // must return an error
          res = TRI_ERROR_INTERNAL;
        }
        return res;
      }

      if (IsWrite(trxCollection->_accessType) &&
          TRI_GetOperationModeServer() == TRI_VOCBASE_MODE_NO_CREATE &&
          !LogicalCollection::IsSystemName(
              trxCollection->_collection->name())) {
        return TRI_ERROR_ARANGO_READ_ONLY;
      }

      // store the waitForSync property
      trxCollection->_waitForSync =
          trxCollection->_collection->waitForSync();
    }

    TRI_ASSERT(trxCollection->_collection != nullptr);

    if (nestingLevel == 0 &&
        IsWrite(trxCollection->_accessType)) {
      // read-lock the compaction lock
      if (!HasHint(this, TransactionHints::Hint::NO_COMPACTION_LOCK)) {
        if (!trxCollection->_compactionLocked) {
          trxCollection->_collection->preventCompaction();
          trxCollection->_compactionLocked = true;
        }
      }
    }

    if (IsWrite(trxCollection->_accessType) &&
        trxCollection->_originalRevision == 0) {
      // store original revision at transaction start
      trxCollection->_originalRevision =
          trxCollection->_collection->revision();
    }

    bool shouldLock = HasHint(this, TransactionHints::Hint::LOCK_ENTIRELY);

    if (!shouldLock) {
      shouldLock = (IsWrite(trxCollection->_accessType) && !IsSingleOperationTransaction(this));
    }

    if (shouldLock && !IsLocked(trxCollection)) {
      // r/w lock the collection
      int res = trxCollection->doLock(trxCollection->_accessType, nestingLevel);

      if (res != TRI_ERROR_NO_ERROR) {
        return res;
      }
    }
  }

  return TRI_ERROR_NO_ERROR;
}

/// @brief release collection locks for a transaction
int TransactionState::unuseCollections(int nestingLevel) {
  int res = TRI_ERROR_NO_ERROR;

  // process collections in reverse order
  for (auto it = _collections.rbegin(); it != _collections.rend(); ++it) {
    TransactionCollection* trxCollection = (*it);

    if (IsLocked(trxCollection) &&
        (nestingLevel == 0 || trxCollection->_nestingLevel == nestingLevel)) {
      // unlock our own r/w locks
      trxCollection->doUnlock(trxCollection->_accessType, nestingLevel);
    }

    // the top level transaction releases all collections
    if (nestingLevel == 0 && trxCollection->_collection != nullptr) {
      if (!HasHint(this, TransactionHints::Hint::NO_COMPACTION_LOCK)) {
        if (IsWrite(trxCollection->_accessType) && trxCollection->_compactionLocked) {
          // read-unlock the compaction lock
          trxCollection->_collection->allowCompaction();
          trxCollection->_compactionLocked = false;
        }
      }

      trxCollection->_lockType = AccessMode::Type::NONE;
    }
  }

  return res;
}
