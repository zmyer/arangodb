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
#include "Cluster/ClusterInfo.h"
#include "Cluster/ServerState.h"
#include "Logger/Logger.h"
#include "Random/RandomGenerator.h"

using namespace arangodb;
using namespace arangodb::transaction;

uint64_t TransactionRegistry::UniqueGenerator::_registryId = 0;


TransactionRegistry::UniqueGenerator::UniqueGenerator(uint64_t n, uint64_t c) :
  _next(n), _last(std::numeric_limits<uint64_t>::max()), _chunks(c) {
  _registryId =
    RandomGenerator::interval(static_cast<uint64_t>(0x0000FFFFFFFFFFFFULL));
}

// offer and burn an id increment by 4
inline TransactionId TransactionRegistry::UniqueGenerator::operator()() {
  MUTEX_LOCKER(guard, _lock);
  if (_next == _last) {
    getSomeNoLock();
  }
  return TransactionId(_registryId, _next+=4);
}

// Get a bunch more ids.
inline void TransactionRegistry::UniqueGenerator::getSomeNoLock() {
  _next = 0;
  _last = _next + _chunks - 1;
  uint64_t r = _next%4; 
  if(r != 0) {
    _next += 4-r;
  }
  r = _last%4;
  if(r != 0) {
    _last -= r;
  }
}

inline uint64_t TransactionRegistry::UniqueGenerator::registryId() const {
  return _registryId;
}

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

/// @brief insert new transaction (coordinator)
TransactionId TransactionRegistry::insert(Methods* transaction, double ttl) {

  TRI_ASSERT(transaction != nullptr);

  auto vocbase = transaction->vocbase();
  auto vocbaseName = vocbase->name();
  auto id = _generator();

  MUTEX_LOCKER(locker, _lock);
  auto m = _transactions.find(vocbaseName);
  if (m == _transactions.end()) {
    m = _transactions.emplace(
      vocbaseName, std::unordered_map<TransactionId, TransactionInfo*>()).first;
    TRI_ASSERT(_transactions.find(vocbaseName) != _transactions.end());
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

    auto vocbaseTrxs = _transactions.find(vocbaseName)->second;
    TRI_ASSERT(vocbaseTrxs.find(id) != vocbaseTrxs.end());
    
  } else {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id already there");
  }
  
  return id;
  
}

/// @brief insert transaction on db servers
void TransactionRegistry::insert(
  TransactionId const& id, Methods* transaction, double ttl) {
  
  TRI_ASSERT(transaction != nullptr);

  auto vocbase = transaction->vocbase();
  auto vocbaseName = vocbase->name();
  
  MUTEX_LOCKER(locker, _lock);
  
  auto m = _transactions.find(vocbaseName);
  if (m == _transactions.end()) {
    m = _transactions.emplace(vocbaseName,
                              std::unordered_map<TransactionId, TransactionInfo*>()).first;
    
    TRI_ASSERT(_transactions.find(vocbaseName) != _transactions.end());
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

    TRI_ASSERT(_transactions.find(vocbaseName)->second.find(id) !=
               _transactions.find(vocbaseName)->second.end());

  } else {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id already there");
  }
}

/// @brief open 
Methods* TransactionRegistry::open(TransactionId const& id, TRI_vocbase_t* vocbase) {
  // std::cout << "Taking out transaction with ID " << id << std::endl;
  MUTEX_LOCKER(locker, _lock);

  TransactionInfo* ti = getInfo(id, vocbase);

  if (ti->_lifeCycle == ABORTED) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id has been aborted already");
  }
  
  if (ti->_lifeCycle == COMMITTED) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id has been committed already");
  }
  
  if (ti->_isOpen) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id is already open");
  }
  
  ti->_isOpen = true;

  return ti->_transaction;
  
}

void TransactionRegistry::closeAbort(TransactionId const& id, double ttl) {
  close(nullptr, id, ttl, ABORTED);
}

void TransactionRegistry::closeCommit(TransactionId const& id, double ttl) {
  close(nullptr, id, ttl, COMMITTED);  
}

/// @brief close
void TransactionRegistry::close(
  TRI_vocbase_t* vocbase, TransactionId const& id, double ttl, LifeCycle lc) {
  
  TransactionInfo* ti = getInfo(id, vocbase);
  
  if (ti->_lifeCycle == ABORTED) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id has been aborted already");
  }
  
  if (ti->_lifeCycle == COMMITTED) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id has been committed already");
  }
  
  if (!ti->_isOpen) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id is not open");
  }

  if (lc == COMMITTED) {
    ti->_transaction->commit();
  } else if (lc == ABORTED) {
    ti->_transaction->abortExternal();
  }

  ti->_isOpen = false;
  ti->_expires = TRI_microtime() + ti->_timeToLive;
  
}

void TransactionRegistry::closeAbort(TRI_vocbase_t* vocbase, TransactionId const& id, double ttl) {
  close(vocbase, id, ttl, ABORTED);
}

/// @brief close
void TransactionRegistry::closeCommit(TRI_vocbase_t* vocbase, TransactionId const& id, double ttl) {
  close(vocbase, id, ttl, COMMITTED);
}

void TransactionRegistry::report(
  TRI_vocbase_t* vocbase, TransactionId const& id, double ttl, LifeCycle lc) {

  MUTEX_LOCKER(locker, _lock);

  auto m = _transactions.find(vocbase->name());
  if (m == _transactions.end()) {
    m = _transactions.emplace(vocbase->name(),
                         std::unordered_map<TransactionId, TransactionInfo*>()).first;
  }
  auto q = m->second.find(id);
  if (q == m->second.end()) {
#warning Needs fixing: transaction being reported, which never existed in the registry
    /*THROW_ARANGO_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
      "transaction with given vocbase and id not found");*/
    LOG_TOPIC(WARN, Logger::TRANSACTIONS) << "Needs fixing: transaction was never in the registry";
    return;
  }
  
  TransactionInfo* ti = q->second;
  
  if (ti->_lifeCycle == ABORTED) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id has been aborted already");
  }
  
  if (ti->_lifeCycle == COMMITTED) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      TRI_ERROR_INTERNAL, "transaction with given vocbase and id has been committed already");
  }
  
  if (lc == COMMITTED) {
    ti->_transaction->commit();
  } else if (lc == ABORTED) {
    ti->_transaction->abortExternal();
  }

  ti->_isOpen = false;
  ti->_expires = TRI_microtime() + ti->_timeToLive;

}

/// @brief close
void TransactionRegistry::reportAbort(Methods* transaction, double ttl) {
  report(transaction->vocbase(), transaction->id(), ttl, ABORTED);
}

/// @brief close
void TransactionRegistry::reportCommit(Methods* transaction, double ttl) {
  report(transaction->vocbase(), transaction->id(), ttl, COMMITTED);
}

/// @brief destroy
void TransactionRegistry::destroy(
  std::string const& vocbase, TransactionId const& id, int errorCode) {

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

  // Now we can delete it:
  //delete ti->_transaction;
  delete ti;

  t->second = nullptr;
  m->second.erase(t);
}

/// @brief destroy
void TransactionRegistry::destroy(
  TRI_vocbase_t* vocbase, TransactionId const& id, int errorCode) {
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
        TransactionInfo*& ti = y.second;
        if (!ti->_isOpen && now > ti->_expires) {
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

TransactionId TransactionRegistry::generateId () {
  return _generator();
}

TransactionRegistry::TransactionInfo* TransactionRegistry::getInfo(
  TransactionId const& id, TRI_vocbase_t* vocbase) const {

  vocbaseEntry::const_iterator e;

  // vocbase not specified.
  if (vocbase == nullptr) {
    bool found = false;
    for (auto const v : _transactions) {
      e = v.second.find(id);
      if (e != v.second.end()) { // Found
        found = true;
        break;
      }
    }
    if (!found) {
      THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given id not found");
    }
  } else {
    auto m = _transactions.find(vocbase->name());
    if (m == _transactions.end()) {
      THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "no transactions found for vocbase");
    }
    e = m->second.find(id);
    if (e == m->second.end()) {
      THROW_ARANGO_EXCEPTION_MESSAGE(
        TRI_ERROR_INTERNAL, "transaction with given vocbase and id not found");
    }
  }

  return e->second;
  
}


void TransactionRegistry::toVelocyPack(VPackBuilder& builder) {
  TRI_ASSERT(builder.isOpenObject());
  // Iterate over vocbases
  for (auto const& vocbase : _transactions) { 
    builder.add(VPackValue(vocbase.first));
    VPackArrayBuilder b(&builder);
    // Iterate over transactions
    for (auto const& transaction : vocbase.second) { 
      transaction.second->toVelocyPack(builder);
    }
  }
}

uint64_t TransactionRegistry::id() const {
  return _generator.registryId();
}
