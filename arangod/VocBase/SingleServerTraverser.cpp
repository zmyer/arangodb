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
/// @author Michael Hackstein
////////////////////////////////////////////////////////////////////////////////

#include "SingleServerTraverser.h"
#include "Basics/StringRef.h"

#include "Aql/AqlValue.h"
#include "Graph/BreadthFirstEnumerator.h"
#include "Transaction/Methods.h"
#include "VocBase/LogicalCollection.h"
#include "VocBase/ManagedDocumentResult.h"
#include "VocBase/TraverserCache.h"

using namespace arangodb;
using namespace arangodb::traverser;
using namespace arangodb::graph;

////////////////////////////////////////////////////////////////////////////////
/// @brief Get a document by it's ID. Also lazy locks the collection.
///        If DOCUMENT_NOT_FOUND this function will return normally
///        with a OperationResult.failed() == true.
///        On all other cases this function throws.
////////////////////////////////////////////////////////////////////////////////
/*
static int FetchDocumentById(transaction::Methods* trx,
                             StringRef const& id,
                             ManagedDocumentResult& result) {
  size_t pos = id.find('/');
  if (pos == std::string::npos) {
    TRI_ASSERT(false);
    return TRI_ERROR_INTERNAL;
  }

  int res = trx->documentFastPathLocal(id.substr(0, pos).toString(),
                                       id.substr(pos + 1).toString(), result);

  if (res != TRI_ERROR_NO_ERROR && res != TRI_ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
    THROW_ARANGO_EXCEPTION(res);
  }
  return res;
}*/

SingleServerEdgeCursor::SingleServerEdgeCursor(ManagedDocumentResult* mmdr,
    transaction::Methods* trx,
    size_t nrCursors, std::vector<size_t> const* mapping)
    : _trx(trx),
      _mmdr(mmdr), 
      _cursors(),
      _currentCursor(0),
      _currentSubCursor(0),
      _cachePos(0),
      _internalCursorMapping(mapping) {
  TRI_ASSERT(_mmdr != nullptr);
  _cursors.reserve(nrCursors);
  _cache.reserve(1000);
};

bool SingleServerEdgeCursor::next(std::function<void(std::string const&, VPackSlice, size_t)> callback) {
  if (_currentCursor == _cursors.size()) {
    return false;
  }
  if (_cachePos < _cache.size()) {
    LogicalCollection* collection = _cursors[_currentCursor][_currentSubCursor]->collection();
    if (collection->readDocument(_trx, _cache[_cachePos++], *_mmdr)) {
      VPackSlice edgeDocument(_mmdr->vpack());
      std::string eid = _trx->extractIdString(edgeDocument);
      if (_internalCursorMapping != nullptr) {
        TRI_ASSERT(_currentCursor < _internalCursorMapping->size());
        callback(eid, edgeDocument, _internalCursorMapping->at(_currentCursor));
      } else {
        callback(eid, edgeDocument, _currentCursor);
      }
    }
    
    return true;
  }
  // We need to refill the cache.
  _cachePos = 0;
  auto cursorSet = _cursors[_currentCursor];
  while (cursorSet.empty()) {
    // Fast Forward to the next non-empty cursor set
    _currentCursor++;
    _currentSubCursor = 0;
    if (_currentCursor == _cursors.size()) {
      return false;
    }
    cursorSet = _cursors[_currentCursor];
  }
  auto cursor = cursorSet[_currentSubCursor];
  // NOTE: We cannot clear the cache,
  // because the cursor expect's it to be filled.
  do {
    if (!cursor->hasMore()) {
      // This one is exhausted, next
      ++_currentSubCursor;
      while (_currentSubCursor == cursorSet.size()) {
        ++_currentCursor;
        _currentSubCursor = 0;
        if (_currentCursor == _cursors.size()) {
          // We are done, all cursors exhausted.
          return false;
        }
        cursorSet = _cursors[_currentCursor];
      }
      cursor = cursorSet[_currentSubCursor];
      // If we switch the cursor. We have to clear the cache.
      _cache.clear();
    } else {
      _cache.clear();
      auto cb = [&] (DocumentIdentifierToken const& token) {
        _cache.emplace_back(token);
      };
      bool tmp = cursor->getMore(cb, 1000);
      TRI_ASSERT(tmp == cursor->hasMore());
    }
  } while (_cache.empty());

  TRI_ASSERT(_cachePos < _cache.size());
  LogicalCollection* collection = cursor->collection();
  if (collection->readDocument(_trx, _cache[_cachePos++], *_mmdr)) {
    VPackSlice edgeDocument(_mmdr->vpack());
    std::string eid = _trx->extractIdString(edgeDocument);
    if (_internalCursorMapping != nullptr) {
      TRI_ASSERT(_currentCursor < _internalCursorMapping->size());
      callback(eid, edgeDocument, _internalCursorMapping->at(_currentCursor));
    } else {
      callback(eid, edgeDocument, _currentCursor);
    }
  }
  return true;
}

bool SingleServerEdgeCursor::readAll(std::unordered_set<VPackSlice>& result,
                                     size_t& cursorId) {
  if (_currentCursor >= _cursors.size()) {
    return false;
  }
  
  if (_internalCursorMapping != nullptr) {
    TRI_ASSERT(_currentCursor < _internalCursorMapping->size());
    cursorId = _internalCursorMapping->at(_currentCursor);
  } else {
    cursorId = _currentCursor;
  }
  
  auto& cursorSet = _cursors[_currentCursor];
  for (auto& cursor : cursorSet) {
    LogicalCollection* collection = cursor->collection(); 
    auto cb = [&] (DocumentIdentifierToken const& token) {
      if (collection->readDocument(_trx, token, *_mmdr)) {
        result.emplace(_mmdr->vpack());
      }
    };
    while (cursor->getMore(cb, 1000)) {
    }
  }
  _currentCursor++;
  return true;
}

void SingleServerEdgeCursor::readAll(std::function<void(std::string const&, arangodb::velocypack::Slice, size_t&)> callback) {
  size_t cursorId = 0;
  for (size_t currentCursor = 0; currentCursor < _cursors.size(); ++currentCursor) {
    if (_internalCursorMapping != nullptr) {
      TRI_ASSERT(_currentCursor < _internalCursorMapping->size());
      cursorId = _internalCursorMapping->at(_currentCursor);
    } else {
      cursorId = _currentCursor;
    }
    auto& cursorSet = _cursors[currentCursor];
    for (auto& cursor : cursorSet) {
      LogicalCollection* collection = cursor->collection(); 
      auto cb = [&] (DocumentIdentifierToken const& token) {
        if (collection->readDocument(_trx, token, *_mmdr)) {
          
          VPackSlice doc(_mmdr->vpack());
          callback(_trx->extractIdString(doc), doc, cursorId);
        }
      };
      while (cursor->getMore(cb, 1000)) {
      }
    }
  }
}

SingleServerTraverser::SingleServerTraverser(TraverserOptions* opts,
                                             transaction::Methods* trx,
                                             ManagedDocumentResult* mmdr)
  : Traverser(opts, trx, mmdr) {}

SingleServerTraverser::~SingleServerTraverser() {}

aql::AqlValue SingleServerTraverser::fetchVertexData(StringRef vid) {
  return _cache->fetchAqlResult(vid);
  
  /*auto it = _vertices.find(id);
  if (it == _vertices.end()) {
    StringRef ref(id);
    int res = FetchDocumentById(_trx, ref, *_mmdr);
    ++_readDocuments;
    if (res != TRI_ERROR_NO_ERROR) {
      return aql::AqlValue(basics::VelocyPackHelper::NullValue());
    }

    uint8_t const* p = _mmdr->vpack();
    _vertices.emplace(id, p);
    return aql::AqlValue(p, aql::AqlValueFromManagedDocument());
  }

  return aql::AqlValue((*it).second, aql::AqlValueFromManagedDocument());*/
}

aql::AqlValue SingleServerTraverser::fetchEdgeData(StringRef edge) {  
  return _cache->fetchAqlResult(edge);
}

void SingleServerTraverser::addVertexToVelocyPack(StringRef vid,
                                                  VPackBuilder& result) {
  _cache->insertIntoResult(vid, result);
}

void SingleServerTraverser::addEdgeToVelocyPack(StringRef edge,
    VPackBuilder& result) {
  _cache->insertIntoResult(edge, result);
  //result.addExternal(edge.begin());
}

void SingleServerTraverser::setStartVertex(std::string const& vid) {
  _startIdBuilder->clear();
  _startIdBuilder->add(VPackValue(vid));
  VPackSlice idSlice = _startIdBuilder->slice();

  if (!vertexMatchesConditions(idSlice, 0)) {
    // Start vertex invalid
    _done = true;
    return;
  }

  _vertexGetter->reset(vid);

  if (_opts->useBreadthFirst) {
    if (_canUseOptimizedNeighbors) {
      _enumerator.reset(new NeighborsEnumerator(this, vid, _opts));
    } else {
      _enumerator.reset(new BreadthFirstEnumerator(this, _startIdBuilder->slice(), _opts));
    }
  } else {
    _enumerator.reset(new DepthFirstEnumerator(this, vid, _opts));
  }
  _done = false;
}

size_t SingleServerTraverser::getAndResetReadDocuments() {
  //size_t tmp = _readDocuments;
  //_readDocuments = 0;
  return this->_cache->getAndResetInsertedDocuments();
}

bool SingleServerTraverser::getVertex(VPackSlice edge,
                                      std::vector<std::string>& result) {
  return _vertexGetter->getVertex(edge, result);
}

bool SingleServerTraverser::getSingleVertex(VPackSlice edge, VPackSlice vertex,
                                            uint64_t depth, VPackSlice& result) {
  return _vertexGetter->getSingleVertex(edge, vertex, depth, result);
}
