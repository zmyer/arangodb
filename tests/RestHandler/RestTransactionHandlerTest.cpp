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

#include "GeneralServer/GeneralServerFeature.h"
#include "RestHandler/RestTransactionHandler.h"
#include "StorageEngine/StorageEngineMock.h"
#include "Transaction/TransactionRegistryMock.h"
#include "V8Server/V8DealerFeatureMock.h"
#include "VocBase/Methods/TransactionsMock.h"
#include "VocBase/vocbase.h"

using namespace arangodb;
using namespace arangodb::basics;

namespace arangodb {
namespace tests {
namespace rest_trans_handler_test {


struct MOCK_vocbase_t : public TRI_vocbase_t {
  MOCK_vocbase_t() {};
};


TEST_CASE("Test pre 3.2", "[unittest]") {
  MockStorageEngine mock_engine;  // must precede MOCK_vocbase_t
  MOCK_vocbase_t mock_database;
  mock_database.forceUse();       // prevent double delete
  MockV8DealerFeature mock_dealer;
  MockTransactions mock_trans;

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

  SECTION("Original TX: succeeds") {
    static const char *valid_old_tx = R"=(
{
  "collections" : {
    "write" : [
      "products",
      "materials"
    ]
  },
  "action" : "function () {var db = require('@arangodb').db;db.products.save({});db.materials.save({});return 'worked!';}"
}
)=";
    std::unordered_map<std::string, std::string> headers;
    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, valid_old_tx,
                                                       strlen(valid_old_tx), headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    // RestTransactionHandler owns and deletes GeneralRequest and GeneralResponse
    std::unique_ptr<RestTransactionHandler> handler(
      new RestTransactionHandler(req, resp));

    req->setRequestType(rest::RequestType::POST);
    RestStatus status = handler->execute();

    CHECK( !status.isFailed() );
    REQUIRE ( resp->responseCode() == rest::ResponseCode::OK);
  } // SECTION
} // TEST_CASE


TEST_CASE("Retrieve Tx Id", "[unittest]") {
  MockStorageEngine mock_engine;  // must precede MOCK_vocbase_t
  MOCK_vocbase_t mock_database;
  mock_database.forceUse();       // prevent double delete

  SECTION("Header only") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;
    headers.insert({RestTransactionHandler::kTransactionHeaderLowerCase, "345-987"});
    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.mock_registry_id_ = 345;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.ok() );
    REQUIRE( 345==tx_id.coordinator );
    REQUIRE( 987==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

  SECTION("URL only") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;

    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    req->addSuffix("32259-2556");

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.mock_registry_id_ = 32259;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.ok() );
    REQUIRE( 32259==tx_id.coordinator );
    REQUIRE( 2556==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

  SECTION("Matching Header & URL") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;

    headers.insert({RestTransactionHandler::kTransactionHeaderLowerCase, "2983-56774"});
    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    req->addSuffix("2983-56774");

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.mock_registry_id_ = 2983;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.ok() );
    REQUIRE( 2983==tx_id.coordinator );
    REQUIRE( 56774==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

  SECTION("Non-matching Header & URL") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;

    headers.insert({RestTransactionHandler::kTransactionHeaderLowerCase, "52330-9865"});
    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    req->addSuffix("2983-56774");

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.mock_registry_id_ = 52330;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.fail() );
    REQUIRE( 0==tx_id.coordinator );
    REQUIRE( 0==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

  SECTION("Wrong Coordinator") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;

    headers.insert({RestTransactionHandler::kTransactionHeaderLowerCase, "2983-56774"});
    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    req->addSuffix("2983-56774");

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.mock_registry_id_ = 1111;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.fail() );
    REQUIRE( 0==tx_id.coordinator );
    REQUIRE( 0==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

  SECTION("Bad Tx ID") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;

    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    req->addSuffix("76998");

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.mock_registry_id_ = 1111;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.fail() );
    REQUIRE( 0==tx_id.coordinator );
    REQUIRE( 0==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

  SECTION("Tx not in registry") {
    MockTransactionRegistry mock_registry;
    transaction::TransactionId tx_id;
    char const body[] = "";
    std::unordered_map<std::string, std::string> headers;

    GeneralRequest* req(HttpRequest::createHttpRequest(ContentType::JSON, body,
                                                       0, headers));
    req->setRequestContext(new VocbaseContext(req, &mock_database), true);
    req->addSuffix("4343-9999");

    GeneralResponse* resp(new HttpResponse(rest::ResponseCode::OK));

    mock_registry.throw_getInfo_ = true;
    mock_registry.mock_registry_id_ = 4343;
    Result rs = RestTransactionHandler::ExtractTransactionId(req, resp, tx_id);

    REQUIRE( rs.fail() );
    REQUIRE( 0==tx_id.coordinator );
    REQUIRE( 0==tx_id.identifier );

    delete resp;
    delete req;
  } // SECTION

} // TEST_CASE


TEST_CASE("Start/Stat/Commit Txn", "[unittest][walkme]") {
  MockStorageEngine mock_engine;  // must precede MOCK_vocbase_t
  MOCK_vocbase_t mock_database;
  mock_database.forceUse();       // prevent double delete
  MockV8DealerFeature mock_dealer;
  MockTransactions mock_trans;

  SECTION("Simple triplet") {
    //
    // start
    //
    static const char *start_tx = R"=(
{
  "collections" : {
    "write" : [
      "products",
      "materials"
    ]
  }
}
)=";
    std::unordered_map<std::string, std::string> headers;
    GeneralRequest* req1(HttpRequest::createHttpRequest(ContentType::JSON, start_tx,
                                                       strlen(start_tx), headers));
    req1->setRequestContext(new VocbaseContext(req1, &mock_database), true);

    GeneralResponse* resp1(new HttpResponse(rest::ResponseCode::OK));

    // RestTransactionHandler owns and deletes GeneralRequest and GeneralResponse
    std::unique_ptr<RestTransactionHandler> handler(
      new RestTransactionHandler(req1, resp1));

    req1->setRequestType(rest::RequestType::POST);
    req1->addSuffix("start");
    RestStatus status = handler->execute();

    CHECK( !status.isFailed() );
    REQUIRE ( resp1->responseCode() == rest::ResponseCode::OK);

    std::string txn_string = resp1->headers()[RestTransactionHandler::kTransactionHeader];
    CHECK( 0!=txn_string.length() );
    handler.reset();
    //
    // status
    //
#if 0 // not yet working
    headers.insert(std::pair<std::string,std::string>(RestTransactionHandler::kTransactionHeaderLowerCase,txn_string));

    GeneralRequest* req2(HttpRequest::createHttpRequest(ContentType::JSON, "",
                                                       0, headers));
    req2->setRequestContext(new VocbaseContext(req2, &mock_database), true);

    GeneralResponse* resp2(new HttpResponse(rest::ResponseCode::OK));

    // RestTransactionHandler owns and deletes GeneralRequest and GeneralResponse
    handler.reset(new RestTransactionHandler(req2, resp2));

    req2->setRequestType(rest::RequestType::GET);
    RestStatus status2 = handler->execute();

    CHECK( !status2.isFailed() );
    REQUIRE ( resp2->responseCode() == rest::ResponseCode::OK);
#endif
  } // SECTION

} // TEST_CASE

}}} // three namespaces
