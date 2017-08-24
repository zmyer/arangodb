
#include "Aql/AqlQueryResultCache.h"
#include "Aql/ExecutionPlan.h"
#include "Aql/QueryCache.h"

#include "Basics/Result.h"
#include "Cluster/ClusterInfo.h"
#include "Cluster/ClusterComm.h"
#include "Cluster/ServerState.h"

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

namespace arangodb {
namespace aql {
namespace cache {

using EN = arangodb::aql::ExecutionNode;

// generates a hashable represenation of an exectution plan for usage on dbserver
// empty string denotes uncacheable query
std::string fakeQueryString(ExecutionPlan const* subPlan){
  LOG_TOPIC(ERR, Logger::FIXME) << "######### subPlan: " << subPlan->toVelocyPack()->toJson();
  std::string result = subPlan->root()->fakeQueryString();
  LOG_TOPIC(ERR, Logger::FIXME) << "######### subPlan String: '" << result <<"'";
  return result;
}



Result properties(VPackBuilder& result) {
  Result rv;

  if(ServerState::instance()->isCoordinator()) {
    ClusterInfo* ci = ClusterInfo::instance();
    if (!ci){
      return rv.reset(TRI_ERROR_INTERNAL, "unable to get ClusterInfo instance");
    }

    auto cc = ClusterComm::instance();
    if (!cc){
      return rv.reset(TRI_ERROR_INTERNAL, "unable to get ClusterComm instance");
    }

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
              result.add(answer);
              return rv;
            } else {
              rv.reset(TRI_ERROR_INTERNAL);
            }
          } else {
            rv.reset(TRI_ERROR_CLUSTER_CONNECTION_LOST, "did not receive cluster comm result");
          }
        } else {
          rv.reset(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
        }
      }
    }
    if (rv.fail()){ return rv; }
  } else {
    auto queryCache = arangodb::aql::QueryCache::instance();
    VPackBuilder cacheResult = queryCache->properties();
    result.add(cacheResult.slice());
  }
  return rv;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock PutApiQueryCacheProperties
////////////////////////////////////////////////////////////////////////////////

// set properites
Result properties(VPackSlice const& properties) {
  Result rv;

  if (!properties.isObject()) {
    return rv.reset(TRI_ERROR_HTTP_BAD_PARAMETER, "expecting a JSON-Object body");
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

    if(rv.fail()){ return rv; };

    auto dbServerIdVec = ci->getCurrentDBServers();
    std::vector<ClusterCommRequest> requests;
    std::string const requestsUrl = "/_api/query-cache/properties";

    auto jsonBody = std::make_shared<std::string>(properties.toJson());

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
            // check further?
          } else {
            rv.reset(TRI_ERROR_INTERNAL);
          }
        } else {
          rv.reset(TRI_ERROR_CLUSTER_CONNECTION_LOST, "did not receive cluster comm result");
        }
      } else {
        rv.reset(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
      }
      // communication to one of the dbservers failed
      // so the overall operation failed
      if (rv.fail()){ return rv; }
    }

  } else {
    auto queryCache = arangodb::aql::QueryCache::instance();

    std::pair<std::string, size_t> cacheProperties;
    queryCache->properties(cacheProperties);

    VPackSlice attribute = properties.get("mode");
    if (attribute.isString()) {
      cacheProperties.first = attribute.copyString();
    }

    attribute = properties.get("maxResults");
    if (attribute.isNumber()) {
      cacheProperties.second = static_cast<size_t>(attribute.getUInt());
    }

    queryCache->setProperties(cacheProperties);
  }
  return rv;
}

Result clear(){
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

    if(rv.fail()){ return rv; };

    auto dbServerIdVec = ci->getCurrentDBServers();
    std::vector<ClusterCommRequest> requests;
    std::string const requestsUrl = "/_api/query-cache";

    auto fakeBody = std::make_shared<std::string>();

    for (auto const& id : dbServerIdVec) {
      requests.emplace_back("server:" + id,
                            arangodb::rest::RequestType::DELETE_REQ,
                            requestsUrl, fakeBody);
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
            // check further?
          } else {
            rv.reset(TRI_ERROR_INTERNAL);
          }
        } else {
          rv.reset(TRI_ERROR_CLUSTER_CONNECTION_LOST, "did not receive cluster comm result");
        }
      } else {
        rv.reset(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
      }
      // communication to one of the dbservers failed
      // so the overall operation failed
      if (rv.fail()){ return rv; }
    }
  } else {
    auto queryCache = arangodb::aql::QueryCache::instance();
    queryCache->invalidate();
  }
  return rv;
}

}
}
}
