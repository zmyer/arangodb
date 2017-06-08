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

#include "ManagedDocumentResult.h"
#include "Aql/AqlValue.h"

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::aql;

void ManagedDocumentResult::clone(ManagedDocumentResult& cloned) const {
  cloned.reset();
  if (_managed) {
    cloned.setManaged(_vpack, _lastRevisionId);
  } else {
    cloned.setUnmanaged(_vpack, _lastRevisionId);
  }
}

//add unmanaged vpack 
void ManagedDocumentResult::setUnmanaged(uint8_t const* vpack, TRI_voc_rid_t revisionId) {
  if (_managed) {
    reset();
  }
  TRI_ASSERT(_length == 0);
  _vpack = const_cast<uint8_t*>(vpack);
  _lastRevisionId = revisionId;
}

void ManagedDocumentResult::setManaged(uint8_t const* vpack, TRI_voc_rid_t revisionId) {
  setManaged(vpack, VPackSlice(vpack).byteSize(), revisionId);
}

void ManagedDocumentResult::setManaged(uint8_t const* vpack, size_t length, TRI_voc_rid_t revisionId) {
  VPackSlice slice(vpack);
  if (_length >= length && _managed) {
    std::memcpy(_vpack, vpack, length);
  } else {
    reset();
    _vpack = new uint8_t[length];
    std::memcpy(_vpack, vpack, length);
    _length = length;
  }
  _lastRevisionId = revisionId;
  _managed = true;
}

void ManagedDocumentResult::reset() noexcept {
  if (_managed) {
    delete[] _vpack;
  }
  _managed = false;
  _length = 0;

  _lastRevisionId = 0;
  _vpack = nullptr;
}

void ManagedDocumentResult::addToBuilder(velocypack::Builder& builder, bool allowExternals) const {
  TRI_ASSERT(!empty());
  if (allowExternals && canUseInExternal()) {
    builder.addExternal(_vpack);
  } else {
    builder.add(velocypack::Slice(_vpack));
  }
}

// @brief Creates an AQLValue with the content of this ManagedDocumentResult
// The caller is responsible to properly destroy() the
// returned value
AqlValue ManagedDocumentResult::createAqlValue() {
  TRI_ASSERT(!empty());
  if (canUseInExternal()) {
    // No need to copy. Underlying structure guarantees that Slices stay
    // valid
    return AqlValue(_vpack, AqlValueFromManagedDocument());
  }
  if (_managed) {
    uint8_t* ptr = _vpack;
    _length = 0;
    _vpack = nullptr;
    _lastRevisionId = 0;
    return AqlValue(AqlValueHintTransferOwnership(ptr));
  }
  // Do copy. Otherwise the slice may go out of scope
  return AqlValue(AqlValueHintCopy(_vpack));
}
