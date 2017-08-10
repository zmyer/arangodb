////////////////////////////////////////////////////////////////////////////////
/// @brief test case for RestTransactionHandler upgrades
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Matthew Von-Maszewski
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////
#include "catch.hpp"
#include "fakeit.hpp"

#include "RestHandler/RestTransactionHandler.h"
#include "GeneralServer/GeneralServerFeature.h"
#include "VocBase/vocbase.h"
#include "StorageEngine/StorageEngineMock.h"

//#include "Agency/AddFollower.h"
//#include "Agency/FailedLeader.h"
//#include "Agency/MoveShard.h"
//#include "Agency/AgentInterface.h"
//#include "Agency/Node.h"
//#include "lib/Basics/StringUtils.h"
//#include "lib/Random/RandomGenerator.h"
//#include <iostream>
//#include <velocypack/Parser.h>
//#include <velocypack/Slice.h>
//#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::basics;
using namespace fakeit;

namespace arangodb {
namespace tests {
namespace rest_trans_handler_test {

//const char *agency =
//#include "AddFollowerTest.json"
//  ;
//const char *todo =
//#include "AddFollowerTestToDo.json"
//  ;

  
struct MOCK_vocbase_t : public TRI_vocbase_t {

  MOCK_vocbase_t() {};
};
  
  
TEST_CASE("Test pre 3.2", "[unittest][walkme]") {
  MockStorageEngine mock_engine;  // must precede MOCK_vocbase_t
  MOCK_vocbase_t mock_database;
  mock_database.forceUse();       // prevent double delete
  
  SECTION("Always fails") {
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;
    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body, 0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    
    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));
    // RestTransactionHandler owns and deletes GeneralRequest and GeneralResponse
    std::unique_ptr<RestTransactionHandler> handler(
      new RestTransactionHandler(req, resp));
    
    // ILLEGAL is default ... but set just to be safe and future proof
    req->setRequestType(rest::RequestType::ILLEGAL);
    RestStatus status = handler->execute();

    CHECK( !status.isFailed() );
    REQUIRE ( resp->responseCode() == rest::ResponseCode::METHOD_NOT_ALLOWED);
  } // SECTION
} // TEST_CASE
  
}}} // three namespaces

