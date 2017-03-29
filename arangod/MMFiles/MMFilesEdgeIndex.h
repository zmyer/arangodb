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
/// @author Dr. Frank Celler
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_MMFILES_EDGE_INDEX_H
#define ARANGOD_MMFILES_EDGE_INDEX_H 1

#include "Basics/AssocMulti.h"
#include "Basics/Common.h"
#include "Indexes/Index.h"
#include "MMFiles/MMFilesIndexElement.h"
#include "VocBase/voc-types.h"
#include "VocBase/vocbase.h"

#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>

namespace arangodb {
namespace basics {
class LocalTaskQueue;
}

typedef arangodb::basics::AssocMulti<arangodb::velocypack::Slice, MMFilesSimpleIndexElement,
                                     uint32_t, false> TRI_MMFilesEdgeIndexHash_t;

class IndexIterator;

class MMFilesEdgeIndex final : public Index {
  friend class MMFilesEdgeIndexIterator;
 public:
  MMFilesEdgeIndex() = delete;

  MMFilesEdgeIndex(TRI_idx_iid_t, arangodb::LogicalCollection*, std::string const&);

  ~MMFilesEdgeIndex();

 public:
  IndexType type() const override { return Index::TRI_IDX_TYPE_EDGE_INDEX; }
  
  char const* typeName() const override { return "edge"; }

  bool allowExpansion() const override { return false; }

  bool canBeDropped() const override { return false; }

  bool isSorted() const override { return false; }

  bool hasSelectivityEstimate() const override { return true; }

  double selectivityEstimate(
      arangodb::StringRef const* = nullptr) const override;

  size_t memory() const override;

  void toVelocyPack(VPackBuilder&, bool) const override;

  void toVelocyPackFigures(VPackBuilder&) const override;

  int insert(transaction::Methods*, TRI_voc_rid_t,
             arangodb::velocypack::Slice const&, bool isRollback) override;

  int remove(transaction::Methods*, TRI_voc_rid_t,
             arangodb::velocypack::Slice const&, bool isRollback) override;

  void batchInsert(transaction::Methods*,
                   std::vector<std::pair<TRI_voc_rid_t, VPackSlice>> const&,
                   arangodb::basics::LocalTaskQueue*) override;

  int unload() override;

  int sizeHint(transaction::Methods*, size_t) override;

  bool hasBatchInsert() const override { return true; }

  bool supportsFilterCondition(arangodb::aql::AstNode const*,
                               arangodb::aql::Variable const*, size_t, size_t&,
                               double&) const override;

  IndexIterator* iteratorForCondition(transaction::Methods*,
                                      ManagedDocumentResult*,
                                      arangodb::aql::AstNode const*,
                                      arangodb::aql::Variable const*,
                                      bool) const override;

  arangodb::aql::AstNode* specializeCondition(
      arangodb::aql::AstNode*, arangodb::aql::Variable const*) const override;

  /// @brief Transform the list of search slices to search values.
  ///        This will multiply all IN entries and simply return all other
  ///        entries.
  void expandInSearchValues(arangodb::velocypack::Slice const,
                            arangodb::velocypack::Builder&) const override;

 private:
  // This functions should only be called by the EdgeIndexIterator

  // Find at most limit many element for the given slice and fill
  // them into buffer
  void lookupByKey(IndexLookupContext* context,
                   arangodb::velocypack::Slice* key,
                   std::vector<MMFilesSimpleIndexElement>& buffer,
                   size_t limit) const;

  // Find at most limit many elements after the given element.
  // All returned elements are guaranteed to have the same
  // indexed value as the given one.
  void lookupByKeyContinue(IndexLookupContext* context,
                           MMFilesSimpleIndexElement const& lastElement,
                           std::vector<MMFilesSimpleIndexElement>& buffer,
                           size_t limit) const;

 private:
  /// @brief create the iterator
  IndexIterator* createEqIterator(transaction::Methods*,
                                  ManagedDocumentResult*,
                                  arangodb::aql::AstNode const*,
                                  arangodb::aql::AstNode const*) const;

  IndexIterator* createInIterator(transaction::Methods*,
                                  ManagedDocumentResult*,
                                  arangodb::aql::AstNode const*,
                                  arangodb::aql::AstNode const*) const;

  /// @brief add a single value node to the iterator's keys
  void handleValNode(VPackBuilder* keys,
                     arangodb::aql::AstNode const* valNode) const;

  MMFilesSimpleIndexElement buildElement(TRI_voc_rid_t, arangodb::velocypack::Slice const& doc) const;

 private:
  /// @brief the hash table for the edges
  TRI_MMFilesEdgeIndexHash_t* _edges;

  /// @brief number of buckets effectively used by the index
  size_t _numBuckets;
};
}

#endif
