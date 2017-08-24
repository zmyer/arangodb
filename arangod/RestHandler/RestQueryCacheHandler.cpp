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

#include "Aql/QueryCache.h"
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

bool RestQueryCacheHandler::clearCache() {
  auto queryCache = arangodb::aql::QueryCache::instance();
  queryCache->invalidate();
    
  VPackBuilder result;
  result.add(VPackValue(VPackValueType::Object));
  result.add("error", VPackValue(false));
  result.add("code", VPackValue((int)rest::ResponseCode::OK));
  result.close();
  generateResult(rest::ResponseCode::OK, result.slice());
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock GetApiQueryCacheProperties
////////////////////////////////////////////////////////////////////////////////

Result RestQueryCacheHandler::readProperties() {
  Result rv;
  if(ServerState::instance()->isCoordinator()) {
    ClusterInfo* ci = ClusterInfo::instance();
    if (!ci){
      rv.reset(TRI_ERROR_INTERNAL, "unable to get ClusterInfo instance");
    }

    auto cc = ClusterComm::instance();
    if (!cc){
      rv.reset(TRI_ERROR_INTERNAL, "unable to get ClusterComm instance");
    }

    if(rv.fail()){ generateError(rv); return rv; };

    auto dbServerIdVec = ci->getCurrentDBServers();
    std::vector<ClusterCommRequest> requests;
    std::string const requestsUrl = "/_api/query-cache/properties";
    auto jsonBody = std::make_shared<std::string>();

    if(!dbServerIdVec.empty()) {
      // we assume that all DBServers have the same
      // configuration what might not be true
      size_t nrDone = 0;
      static double const CL_DEFAULT_TIMEOUT = 120.0;
      requests.emplace_back("server:" + dbServerIdVec[0],
                            arangodb::rest::RequestType::GET,
                            requestsUrl, jsonBody);
      cc->performRequests(requests, CL_DEFAULT_TIMEOUT, nrDone, Logger::QUERIES, true);
      if (!requests.empty()) {
        auto& res = requests[0].result;
        if (res.status == CL_COMM_RECEIVED) {
          if (res.answer_code == arangodb::rest::ResponseCode::OK) {
            VPackSlice answer = res.answer->payload();
            if (answer.isObject()) {
              generateResult(rest::ResponseCode::OK, answer);
            } else {
              rv.reset(TRI_ERROR_INTERNAL);
            }
          } else {
            generateError(res.answer_code, TRI_ERROR_CLUSTER_CONNECTION_LOST, "did not receive cluster comm result");
            rv.reset(TRI_ERROR_CLUSTER_CONNECTION_LOST);
            return rv;
          }
        } else {
          rv.reset(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
        }
      }
    }
    if (rv.fail()){ generateError(rv); return rv; }
  } else {
    auto queryCache = arangodb::aql::QueryCache::instance();
    VPackBuilder result = queryCache->properties();
    generateResult(rest::ResponseCode::OK, result.slice());
  }
  return rv;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock PutApiQueryCacheProperties
////////////////////////////////////////////////////////////////////////////////

Result RestQueryCacheHandler::replaceProperties() {
  Result rv;
  auto const& suffixes = _request->suffixes();

  if (suffixes.size() != 1 || suffixes[0] != "properties") {
    generateError(rest::ResponseCode::BAD,
                  TRI_ERROR_HTTP_BAD_PARAMETER,
                  "expecting PUT /_api/query-cache/properties");
    return true;
  }
  bool validBody = true;
  std::shared_ptr<VPackBuilder> parsedBody =
      parseVelocyPackBody(validBody);

  if (!validBody) {
    // error message generated in parseJsonBody
    return true;
  }
  VPackSlice body = parsedBody.get()->slice();

  if (!body.isObject()) {
    generateError(rest::ResponseCode::BAD,
                  TRI_ERROR_HTTP_BAD_PARAMETER, "expecting a JSON-Object body");
    return true;
  }

  if(ServerState::instance()->isCoordinator()) {
    // Ask DB Servers to enable cache 

    ClusterInfo* ci = ClusterInfo::instance();
    if (!ci){
      rv.reset(TRI_ERROR_INTERNAL, "unable to get ClusterInfo instance");
    }

    auto cc = ClusterComm::instance();
    if (!cc){
      rv.reset(TRI_ERROR_INTERNAL, "unable to get ClusterComm instance");
    }

    if(rv.fail()){ generateError(rv); return rv; };

    auto dbServerIdVec = ci->getCurrentDBServers();
    std::vector<ClusterCommRequest> requests;
    std::string const requestsUrl = "/_api/query-cache/properties";

    auto jsonBody = std::make_shared<std::string>(body.toJson());

    for (auto const& id : dbServerIdVec) {
      requests.emplace_back("server:" + id,
                            arangodb::rest::RequestType::PUT,
                            requestsUrl, jsonBody);
    }

    size_t nrDone = 0;
    static double const CL_DEFAULT_TIMEOUT = 120.0;
    cc->performRequests(requests, CL_DEFAULT_TIMEOUT, nrDone, Logger::QUERIES, true);

    for (auto& req : requests) {
      auto& res = req.result;
      if (res.status == CL_COMM_RECEIVED) {
        if (res.answer_code == arangodb::rest::ResponseCode::OK) {
          VPackSlice answer = res.answer->payload();
          if (answer.isObject()) {
            // fine
          } else {
            rv.reset(TRI_ERROR_INTERNAL);
          }
        } else {
          generateError(res.answer_code, TRI_ERROR_CLUSTER_CONNECTION_LOST, "did not receive cluster comm result");
          rv.reset(TRI_ERROR_CLUSTER_CONNECTION_LOST);
          return rv;
        }
      } else {
        rv.reset(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
      }
      // communication to one of the dbservers failed
      // so the overall operation failed
      if (rv.fail()){ generateError(rv); return rv; }
    }

  } else {
    auto queryCache = arangodb::aql::QueryCache::instance();

    std::pair<std::string, size_t> cacheProperties;
    queryCache->properties(cacheProperties);

    VPackSlice attribute = body.get("mode");
    if (attribute.isString()) {
      cacheProperties.first = attribute.copyString();
    }

    attribute = body.get("maxResults");

    if (attribute.isNumber()) {
      cacheProperties.second = static_cast<size_t>(attribute.getUInt());
    }

    queryCache->setProperties(cacheProperties);
  }
  return readProperties();
}
