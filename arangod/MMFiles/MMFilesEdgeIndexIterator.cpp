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

#include "MMFilesEdgeIndexIterator.h"

#include "Basics/StaticStrings.h"
#include "MMFiles/MMFilesEdgeIndex.h"
#include "MMFiles/MMFilesToken.h"
#include "Transaction/Context.h"
#include "Transaction/Methods.h"
#include "VocBase/LogicalCollection.h"

#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;

MMFilesEdgeIndexIterator::MMFilesEdgeIndexIterator(LogicalCollection* collection, transaction::Methods* trx,
                                     ManagedDocumentResult* mmdr,
                                     arangodb::MMFilesEdgeIndex const* index,
                                     std::unique_ptr<VPackBuilder>& keys)
    : IndexIterator(collection, trx, mmdr, index),
      _index(index),
      _keys(keys.get()),
      _iterator(_keys->slice()),
      _posInBuffer(0),
      _batchSize(1000),
      _lastElement() {
  keys.release();  // now we have ownership for _keys
}

MMFilesEdgeIndexIterator::~MMFilesEdgeIndexIterator() {
  if (_keys != nullptr) {
    // return the VPackBuilder to the transaction context
    _trx->transactionContextPtr()->returnBuilder(_keys.release());
  }
}


bool MMFilesEdgeIndexIterator::next(TokenCallback const& cb, size_t limit) {
  if (limit == 0 || (_buffer.empty() && !_iterator.valid())) {
    // No limit no data, or we are actually done. The last call should have returned false
    TRI_ASSERT(limit > 0); // Someone called with limit == 0. Api broken
    return false;
  }
  while (limit > 0) {
    if (_buffer.empty()) {
      // We start a new lookup
      _posInBuffer = 0;

      VPackSlice tmp = _iterator.value();
      if (tmp.isObject()) {
        tmp = tmp.get(StaticStrings::IndexEq);
      }
      _index->lookupByKey(&_context, &tmp, _buffer, _batchSize);
    } else if (_posInBuffer >= _buffer.size()) {
      // We have to refill the buffer
      _buffer.clear();

      _posInBuffer = 0;
      _index->lookupByKeyContinue(&_context, _lastElement, _buffer, _batchSize);
    }

    if (_buffer.empty()) {
      _iterator.next();
      _lastElement = MMFilesSimpleIndexElement();
      if (!_iterator.valid()) {
        return false;
      }
    } else {
      _lastElement = _buffer.back();
      // found something
      cb(MMFilesToken{_buffer[_posInBuffer++].revisionId()});
      limit--;
    }
  }
  return true;
}

void MMFilesEdgeIndexIterator::reset() {
  _posInBuffer = 0;
  _buffer.clear();
  _iterator.reset();
  _lastElement = MMFilesSimpleIndexElement();
}
 
