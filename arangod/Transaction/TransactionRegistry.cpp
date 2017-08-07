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
/// @author Kaveh Vaheidpour
/// @author Max Neunhoeffer
////////////////////////////////////////////////////////////////////////////////

#include "TransactionRegistry.h"
#include "Methods.h"
#include "Basics/ReadLocker.h"
#include "Basics/WriteLocker.h"
#include "Cluster/CollectionLockState.h"
#include "Logger/Logger.h"

using namespace arangodb;
using namespace arangodb::transaction;

/// @brief destroy all open transactions
TransactionRegistry::~TransactionRegistry() {
  std::vector<std::pair<std::string, TransactionId>> toDelete;

  {
    MUTEX_LOCKER(locker, _lock);

    try {
      for (auto& x : _transactions) {
        // x.first is a TRI_vocbase_t* and
        // x.second is a std::unordered_map<TransactionId, TransactionInfo*>
        for (auto& y : x.second) {
          // y.first is a TransactionId and
          // y.second is a TransactionInfo*
          toDelete.emplace_back(x.first, y.first);
        }
      }
    } catch (...) {
      // the emplace_back() above might fail
      // prevent throwing exceptions in the destructor
    }
  }

  // note: destroy() will acquire _lock itself, so it must be called without
  // holding the lock
  for (auto& p : toDelete) {
    try {  // just in case
      destroy(p.first, p.second, TRI_ERROR_TRANSACTION_ABORTED);
    } catch (...) { }
  }
}

/// @brief insert
void TransactionRegistry::insert(TransactionId id, Methods* transaction, double ttl) {
  TRI_ASSERT(transaction != nullptr);

  auto vocbase = transaction->vocbase();

  MUTEX_LOCKER(locker, _lock);

  auto m = _transactions.find(vocbase->name());
  if (m == _transactions.end()) {
    m = _transactions.emplace(vocbase->name(),
                         std::unordered_map<TransactionId, TransactionInfo*>()).first;

    TRI_ASSERT(_transactions.find(vocbase->name()) != _transactions.end());
  }
  auto t = m->second.find(id);
  if (t == m->second.end()) {
    auto p = std::make_unique<TransactionInfo>();
    p->_vocbase = vocbase;
    p->_id = id;
    p->_transaction = transaction;
    p->_isOpen = false;
    p->_timeToLive = ttl;
    p->_expires = TRI_microtime() + ttl;
    m->second.emplace(id, p.get());
    p.release();

    TRI_ASSERT(_transactions.find(vocbase->name())->second.find(id) !=
               _transactions.find(vocbase->name())->second.end());

  } else {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id already there");
  }
}

/// @brief open
Methods* TransactionRegistry::open(TRI_vocbase_t* vocbase, TransactionId id) {
  // std::cout << "Taking out transaction with ID " << id << std::endl;
  MUTEX_LOCKER(locker, _lock);

  auto m = _transactions.find(vocbase->name());
  if (m == _transactions.end()) {
    return nullptr;
  }
  auto q = m->second.find(id);
  if (q == m->second.end()) {
    return nullptr;
  }
  TransactionInfo* qi = q->second;
  if (qi->_isOpen) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id is already open");
  }
  qi->_isOpen = true;

  return qi->_transaction;
}

/// @brief close
void TransactionRegistry::close(TRI_vocbase_t* vocbase, TransactionId id, double ttl) {
  // std::cout << "Returning transaction with ID " << id << std::endl;
  MUTEX_LOCKER(locker, _lock);

  auto m = _transactions.find(vocbase->name());
  if (m == _transactions.end()) {
    m = _transactions.emplace(vocbase->name(),
                         std::unordered_map<TransactionId, TransactionInfo*>()).first;
  }
  auto q = m->second.find(id);
  if (q == m->second.end()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                   "transaction with given vocbase and id not found");
  }
  TransactionInfo* qi = q->second;
  if (!qi->_isOpen) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id is not open");
  }

  
  qi->_isOpen = false;
  qi->_expires = TRI_microtime() + qi->_timeToLive;
}

/// @brief close
void TransactionRegistry::closeAbort(TRI_vocbase_t* vocbase, TransactionId id, double ttl) {
  
}

/// @brief close
void TransactionRegistry::closeCommit(TRI_vocbase_t* vocbase, TransactionId id, double ttl) {
  
}

/// @brief destroy
void TransactionRegistry::destroy(
  std::string const& vocbase, TransactionId id, int errorCode) {

  MUTEX_LOCKER(locker, _lock);

  auto m = _transactions.find(vocbase); // All transactions for vocbase in registry
  if (m == _transactions.end()) {       // Not found: create registry for vocbase
    m = _transactions.emplace(
      vocbase, std::unordered_map<TransactionId, TransactionInfo*>()).first;
  }
  
  auto t = m->second.find(id);          // Find transaction with id in vocbase
  if (t == m->second.end()) {           // Not found: error!
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id not found");
  }
  TransactionInfo* ti = t->second;

  if (ti->_isOpen) {
    ti->_transaction->abortExternal(); // Thread safe abort
    return;
  }

  if (errorCode == TRI_ERROR_NO_ERROR) {
    // commit the operation
    ti->_transaction->commit();
  }

  // Now we can delete it:
  delete ti->_transaction;
  delete ti;

  t->second = nullptr;
  m->second.erase(t);
}

/// @brief destroy
void TransactionRegistry::destroy(TRI_vocbase_t* vocbase, TransactionId id, int errorCode) {
  destroy(vocbase->name(), id, errorCode);
}

/// @brief expireTransactions
void TransactionRegistry::expireTransactions() {
  double now = TRI_microtime();
  std::vector<std::pair<std::string, TransactionId>> toDelete;

  {
    MUTEX_LOCKER(locker, _lock);
    for (auto& x : _transactions) {
      // x.first is a TRI_vocbase_t* and
      // x.second is a std::unordered_map<TransactionId, TransactionInfo*>
      for (auto& y : x.second) {
        // y.first is a TransactionId and
        // y.second is a TransactionInfo*
        TransactionInfo*& qi = y.second;
        if (!qi->_isOpen && now > qi->_expires) {
          toDelete.emplace_back(x.first, y.first);
        }
      }
    }
  }

  for (auto& p : toDelete) {
    try {  // just in case
      destroy(p.first, p.second, TRI_ERROR_TRANSACTION_ABORTED);
    } catch (...) {
    }
  }
}

/// @brief return number of registered transactions
size_t TransactionRegistry::numberRegisteredTransactions() {
  MUTEX_LOCKER(lock, _lock);
  size_t sum = 0;
  for (auto const&m : _transactions) {
    sum += m.second.size();
  }
  return sum;
}

/// @brief for shutdown, we need to shut down all transactions:
void TransactionRegistry::destroyAll() {
  std::vector<std::pair<std::string, TransactionId>> allTransactions;
  {
    MUTEX_LOCKER(lock, _lock);
    for (auto& p : _transactions) {
      for (auto& q : p.second) {
        allTransactions.emplace_back(p.first, q.first);
      }
    }
  }
  for (auto& p : allTransactions) {
    try {
      destroy(p.first, p.second, TRI_ERROR_SHUTTING_DOWN);
    } catch (...) {
      // ignore any errors here
    }
  }
}
