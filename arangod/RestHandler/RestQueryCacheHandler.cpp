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
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "Aql/AqlQueryResultCache.h"
#include "Cluster/ClusterInfo.h"
#include "Cluster/ClusterComm.h"
#include "Cluster/ServerState.h"
#include "Rest/HttpRequest.h"
#include "RestQueryCacheHandler.h"
#include "Basics/error.h"

#include <string>

using namespace arangodb;
using namespace arangodb::aql;
using namespace arangodb::basics;
using namespace arangodb::rest;

RestQueryCacheHandler::RestQueryCacheHandler(GeneralRequest* request,
                                             GeneralResponse* response)
    : RestVocbaseBaseHandler(request, response) {}

bool RestQueryCacheHandler::isDirect() const { return false; }

RestStatus RestQueryCacheHandler::execute() {
  // extract the sub-request type
  auto const type = _request->requestType();

  switch (type) {
    case rest::RequestType::DELETE_REQ:
      clearCache();
      break;
    case rest::RequestType::GET:
      readProperties();
      break;
    case rest::RequestType::PUT:
      replaceProperties();
      break;
    default:
      generateNotImplemented("ILLEGAL " + DOCUMENT_PATH);
      break;
  }

  // this handler is done
  return RestStatus::DONE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock DeleteApiQueryCache
////////////////////////////////////////////////////////////////////////////////

Result RestQueryCacheHandler::clearCache() {
  VPackBuilder result;
  Result rv = arangodb::aql::cache::clear();
  if (rv.fail()){ generateError(rv); return rv; }
  generateSuccess(rest::ResponseCode::OK,VPackSlice::noneSlice(), true /*ignore result*/);
  return rv;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock GetApiQueryCacheProperties
////////////////////////////////////////////////////////////////////////////////

Result RestQueryCacheHandler::readProperties() {
  VPackBuilder result;
  Result rv = arangodb::aql::cache::properties(result); //get properties
  if (rv.fail()){ generateError(rv); return rv; }
  generateResult(rest::ResponseCode::OK, result.slice());
  return rv;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock PutApiQueryCacheProperties
////////////////////////////////////////////////////////////////////////////////

Result RestQueryCacheHandler::replaceProperties() {
  Result rv;
  auto const& suffixes = _request->suffixes();

  if (suffixes.size() != 1 || suffixes[0] != "properties") {
    rv.reset(TRI_ERROR_HTTP_BAD_PARAMETER, "expecting PUT /_api/query-cache/properties");
    generateError(rv);
    return rv;
  }

  bool validBody = true;
  std::shared_ptr<VPackBuilder> parsedBody = parseVelocyPackBody(validBody);

  if (!validBody) {
    rv.reset(TRI_ERROR_HTTP_BAD_PARAMETER, "bad json body");
    generateError(rv);
    return rv;
  }

  VPackSlice body = parsedBody.get()->slice();
  rv = arangodb::aql::cache::properties(body); //set properties
  if (rv.fail()){ generateError(rv); return rv; }
  return readProperties();
}
