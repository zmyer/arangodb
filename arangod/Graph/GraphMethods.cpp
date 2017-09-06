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

#include "GraphMethods.h"

#include "Aql/FixedVarExpressionContext.h"

#include "Graph/TraverserCache.h"
#include "Graph/TraverserCacheFactory.h"

#include <velocypack/Builder.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::graph;

void GraphMethods::clearVariableValues() {
  _ctx->clearVariableValues();
}

void GraphMethods::setVariableValue(aql::Variable const* var,
                                   aql::AqlValue const value) {
  _ctx->setVariableValue(var, value);
}

void GraphMethods::serializeVariables(VPackBuilder& builder) const {
  TRI_ASSERT(builder.isOpenArray());
  _ctx->serializeAllVariables(_trx, builder);
}

TraverserCache* GraphMethods::cache() const {
  TRI_ASSERT(_cache != nullptr);
  return _cache.get();
}

void GraphMethods::activateCache(
    bool enableDocumentCache,
    std::unordered_map<ServerID, traverser::TraverserEngineID> const* engines) {
  // Do not call this twice.
  TRI_ASSERT(_cache == nullptr);
  _cache.reset(cacheFactory::CreateCache(_trx, enableDocumentCache, engines));
}
