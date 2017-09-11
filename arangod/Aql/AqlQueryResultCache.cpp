#include "Aql/AqlQueryResultCache.h"
#include "Aql/ExecutionPlan.h"
#include "Aql/QueryCache.h"
#include "Aql/AqlItemBlock.h"

#include "Basics/Result.h"
#include "Basics/encoding.h"
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
  std::string result;
  bool stringValid = subPlan->root()->fakeQueryString(result);
  //LOG_TOPIC(ERR, Logger::FIXME) << "### ### subPlan String: '" << result <<"' " << std::boolalpha << stringValid;
  if (!stringValid) {
    result.clear();
  }
  return result;
}

auto vPackToBlock(VPackArrayIterator& iter
                 ,std::unique_ptr<ResourceMonitor>& monitor
                 ,std::size_t atLeast, std::size_t atMost
                 )
  -> std::tuple<Result, std::unique_ptr<AqlItemBlock>, std::size_t>
{
  //tries to read `items` items from `iter` into a default inititalized AqlItemBlock `block`
  std::tuple<Result,std::unique_ptr<AqlItemBlock>, std::size_t> result{TRI_ERROR_NO_ERROR,nullptr,0};

  Result& rv = std::get<0>(result);
  std::unique_ptr<AqlItemBlock>& block = std::get<1>(result);
  std::size_t& items = std::get<2>(result);

  if(atLeast && !iter.valid()){
    rv.reset(TRI_ERROR_INTERNAL,"try to get items from invalid iterator");
  }

  bool skip = (monitor == nullptr);

  TRI_ASSERT(iter.value().isArray());
  auto max = std::min(atMost,iter.size()-iter.index());

  if(!skip){
    block.reset(new AqlItemBlock(monitor.get()
                                ,max /*nrItems*/
                                ,iter.value().length() /*nrRegs*/
                                ));
  }

  std::size_t itemsReceived = 0;
  try {
    while(itemsReceived < max && iter.valid()) {
      if(!skip){
        std::size_t reg=0;
        for(auto const& value : VPackArrayIterator(iter.value())){
          if(value.isCustom()){
            uint8_t const* p = value.startAs<uint8_t>();
            if(*p == 0xf6){
              LOG_DEVEL << "unpack custom";
              p+=2; //skip type and len;
              int64_t low = encoding::readNumber<int64_t>(p,sizeof(std::int64_t));
              p += sizeof(std::int64_t);
              int64_t high = encoding::readNumber<int64_t>(p,sizeof(std::int64_t));
              block->emplaceValue(itemsReceived, reg, low,high);
              //block->setValue(itemsReceived, reg, AqlValue(low,high));
            } else {
              THROW_ARANGO_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL, "unknown custom type");
            }
          } else if(!value.isNone()){
            block->emplaceValue(itemsReceived, reg, value);
            //block->setValue(itemsReceived, reg, AqlValue(value));
          }
          ++reg;
        }
      }
      ++itemsReceived;
      iter.next();
    }
  } CATCH_TO_RESULT(rv)
  items=itemsReceived;
  LOG_DEVEL_IF(rv.fail()) << "!!!!!!!!!!" <<rv.errorMessage();
  return result;
}

Result blockToVPack(AqlItemBlock const& block, VPackBuilder& builder, std::size_t regs /*0 if unknown*/){
  // this functions writes rows (results) of an AqlItemBlock as Arrays to an
  // already open VPackBuilder.
  // 
  // the following translation is applied:
  //    empty -> nullSlice
  //    range -> "peter"
  //    slice (untranslated)


  // due to lack of discussion a pragmatic implementation will be used
  // this will be slow because the data in aql blocks is stored per column
  // and we need to access rows. This is a hard requirement because we need
  // to append data. While the itemblock has chosen the columnise layout
  // to store the data more effective. This is expected to be slow. Especially
  // if we need to create later the "raw" VelocyPack that is used in the remote
  // (coordinator) to create a new itemblock.
  Result rv;
  {
    std::size_t n = block.getNrRegs();
    if(regs){
      if (regs != n ){
        TRI_ASSERT(false);
        return rv.reset(TRI_ERROR_INTERNAL, "number of registers in AqlItemBlock does not match expected value");
      }
    } else {
      regs = n;
    }
  }
  try {
    for(std::size_t i = 0; i < block.size() /*number of items in block*/ ; i++){
      builder.openArray();
      for(std::size_t r=0 ; r < regs /*number of vars in block*/; r++){
        AqlValue const& val = block.getValueReference(i,r);
        if(val.isRange()){

          uint8_t* p = builder.add(VPackValuePair(18ULL, VPackValueType::Custom));
          // 1 - customtype 0xf6
          // 1 - length of payload
          // 8 - low  (int64_t)
          // 8 - high (int64_t)
          *p++ = 0xf6;  // custom type for range
          *p++ = 0x10;  // 16byte to store
          encoding::storeNumber(p, val.range()->_low, sizeof(std::int64_t));
          p += sizeof(std::int64_t);
          encoding::storeNumber(p, val.range()->_high, sizeof(std::int64_t));

        } else if(val.isDocvec()){
          LOG_DEVEL << " ############### not implemented for docvec ###################";
          builder.add(VPackSlice::noneSlice());
          return rv.reset(TRI_ERROR_INTERNAL, "caching of docvec is not supported");
        } else if(val.isEmpty()) {
          builder.add(VPackSlice::noneSlice());
        } else {
          builder.add(val.slice());
        }
      }
      builder.close();
    }
  } CATCH_TO_RESULT(rv)
  LOG_DEVEL_IF(rv.fail()) << "!!!!!!!!!!" <<rv.errorMessage();
  return rv;
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
