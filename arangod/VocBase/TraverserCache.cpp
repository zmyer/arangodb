////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017-2017 ArangoDB GmbH, Cologne, Germany
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

#include "TraverserCache.h"

#include "Cache/CacheManagerFeature.h"
#include "Cache/Common.h"

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::traverser;

TraverserCache::TraverserCache(transction::Methods* trx)
    : _cache(nullptr), _trx(trx) {
  auto cacheManager = CacheManagerFeature::MANAGER;
  TRI_ASSERT(cacheManager != nullptr);
  _cache = cacheManager->createCache(cache::CacheType::Plain);
}

TraverserCache::~TraverserCache() {
  auto cacheManager = CacheManagerFeature::MANAGER;
  // TODO REMOVE ME
  LOG_TOPIC(ERR, arangodb::Logger::FIXME) << "Traverser-Cache used in total "
                                          << _cache->size();
  cacheManager->destroyCache(_cache);
}

// @brief Only for internal use, Cache::Finding prevents
// the cache from removing this specific object. Should not be retained
// for a longer period of time.
// DO NOT give it to a caller.
cache::Finding TraverserCache::lookup(VPackSlice const& idString) {
  TRI_ASSERT(idString.isString());
  void const* key = idString.begin();
  uint32_t keySize = static_cast<uint32_t>(idString.length());
  return _cache.find(key, keySize);
}

TraverserCache::lookupInCollection(arangodb::velocypack::Slice const& idString) {
  TRI_ASSERT(idString.isString());
  StringRef id(idString);
  size_t pos = id.find('/');
  if (pos == std::string::npos) {
    TRI_ASSERT(false);
    return TRI_ERROR_INTERNAL;
  }

  int res = _trx->documentFastPathLocal(id.substr(0, pos).toString(),
                                        id.substr(pos + 1).toString(), _mmdr);

  if (res != TRI_ERROR_NO_ERROR && res != TRI_ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
    // ok we are in a rather bad state. Better throw and abort.
    THROW_ARANGO_EXCEPTION(res);
  }

  VPackSlice result;
  if (res == TRI_ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
    // This is expected, we may have dangling edges. Interpret as NULL
    result = basics::VelocyPackHelper::NullValue();
  } else {
    result = VPackSlice(_mmdr.vpack());
  }

  // TODO Insert result into the cache.
  
  return res;
}

void TraverserCache::insertIntoResult(VPackSlice const& idString,
                                      VPackBuilder& builder) {
  auto finding = lookup(token);
  if (finding.found()) {
    auto val = finding.value();
    VPackSlice slice(val.value());
    // finding makes sure that slice contant stays valid.
    builder.add(slice);
  } else {
    // Not in cache. Fetch and insert.
    VPackSlice slice = lookupInCollection(idString);
    builder.add(slice);
  }
}

bool validateFilter(VPackSlice const& idString,
                    std::function<bool(VPackSlice const&)> filterFunc) {
  auto finding = lookup(token);
  if (finding.found()) {
    auto val = finding.value();
    VPackSlice slice(val.value());
    // finding makes sure that slice contant stays valid.
    return filterFunc(slice);
  }
  // Not in cache. Fetch and insert.
  VPackSlice slice = lookupInCollection(idString);
  return filterFunc(slice);
}
