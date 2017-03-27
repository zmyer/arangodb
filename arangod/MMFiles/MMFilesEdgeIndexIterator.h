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

#ifndef ARANGOD_MMFILES_EDGE_INDEX_ITERATOR_H
#define ARANGOD_MMFILES_EDGE_INDEX_ITERATOR_H 1

#include "Basics/AssocMulti.h"
#include "Basics/Common.h"
#include "Indexes/IndexIterator.h"
#include "MMFiles/MMFilesIndexElement.h"

#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>

namespace arangodb {

class MMFilesEdgeIndex;
  
typedef arangodb::basics::AssocMulti<arangodb::velocypack::Slice, MMFilesSimpleIndexElement,
                                     uint32_t, false> TRI_MMFilesEdgeIndexHash_t;

class MMFilesEdgeIndexIterator final : public IndexIterator {
 public:
  MMFilesEdgeIndexIterator(LogicalCollection* collection, transaction::Methods* trx,
                    ManagedDocumentResult* mmdr,
                    arangodb::MMFilesEdgeIndex const* index,
                    TRI_MMFilesEdgeIndexHash_t const* indexImpl,
                    std::unique_ptr<VPackBuilder>& keys);

  ~MMFilesEdgeIndexIterator();
  
  char const* typeName() const override { return "edge-index-iterator"; }

  bool next(TokenCallback const& cb, size_t limit) override;

  void reset() override;

 private:
  TRI_MMFilesEdgeIndexHash_t const* _index;
  std::unique_ptr<arangodb::velocypack::Builder> _keys;
  arangodb::velocypack::ArrayIterator _iterator;
  std::vector<MMFilesSimpleIndexElement> _buffer;
  size_t _posInBuffer;
  size_t _batchSize;
  MMFilesSimpleIndexElement _lastElement;
};


} // namespace arangodb

#endif
