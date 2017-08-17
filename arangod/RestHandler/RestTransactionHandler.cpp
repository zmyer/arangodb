////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "RestTransactionHandler.h"
#include "Basics/WriteLocker.h"
#include "Basics/ReadLocker.h"

#include "ApplicationFeatures/ApplicationServer.h"
#include "Rest/HttpRequest.h"
#include "Basics/voc-errors.h"
#include "RestServer/TransactionRegistryFeature.h"
#include "Transaction/TransactionRegistry.h"
#include "Transaction/types.h"
#include "V8Server/V8Context.h"
#include "V8Server/V8DealerFeature.h"

#include <velocypack/Builder.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::rest;

executeTransaction_t* RestTransactionHandler::_executeTransactionPtr = & executeTransaction;

char const * RestTransactionHandler::kTransactionHeader          = "X-ArangoDB-Trx";
char const * RestTransactionHandler::kTransactionHeaderLowerCase = "x-arangodb-trx";


RestTransactionHandler::RestTransactionHandler(GeneralRequest* request, GeneralResponse* response)
  : RestVocbaseBaseHandler(request, response)
  , _v8Context(nullptr)
  , _lock()
{}


void RestTransactionHandler::returnContext(){
    WRITE_LOCKER(writeLock, _lock);
    V8DealerFeature::DEALER->exitContext(_v8Context);
    _v8Context = nullptr;
}


RestStatus RestTransactionHandler::execute() {

  switch(_request->requestType()) {

    // original single operation transaction and
    //  start for multi-operation transaction
    case rest::RequestType::POST: {
      executePost();
      break;
    }

    // commit for multi-operation transaction
    case rest::RequestType::PUT: {
      executePut();
      break;
    }

    // status of given ID or list of all
    case rest::RequestType::GET: {
#if 0
#include "transaction/TransactionRegistry.h"
#include "transaction/TransactionRegistryFeature.h"
auto transactionRegistry = TransactionRegistryFeature::TRANSACTION_REGISTRY;

VPackBuilder builder;
{
  VPackObjectBuilder b(&builder)
  transactionRegistry->toVelocyPack(builder);
}

... or ...

auto transactionRegistry = TransactionRegistryFeature::TRANSACTION_REGISTRY;
VPackBuilder builder;
{
  VPackObjectBuilder b(&builder)
  transactionRegistry(transactionId[, vocbase])->toVelocyPack(builder);
}
#endif
      break;
    }

    // abort given ID
    case rest::RequestType::DELETE_REQ: {
      executeDelete();
      break;
    }

    // oops, not supported
    default: {
      generateError(rest::ResponseCode::METHOD_NOT_ALLOWED, 405);
      break;
    }
  } //switch

  return RestStatus::DONE;
}


bool RestTransactionHandler::cancel() {
  //cancel v8 transaction
  WRITE_LOCKER(writeLock, _lock);
  _canceled.store(true);
  auto isolate = _v8Context->_isolate;
  if (!v8::V8::IsExecutionTerminating(isolate)) {
      v8::V8::TerminateExecution(isolate);
  }
  return true;
}


/// @brief static function for extracting and verifying ID from header and/or url
Result RestTransactionHandler::ExtractTransactionId(
  GeneralRequest* request,
  GeneralResponse* response,
  transaction::TransactionId & tx_id) {

  Result rv;
  std::string header_value, url_value, selected_value;
  std::string key(kTransactionHeaderLowerCase);

  selected_value.clear();
  auto header = request->headers().find(key);
  if (request->headers().end() != header) {
    header_value = header->second;
  } // if

  if (1==request->suffixes().size()) {
    url_value = request->suffixes()[0];
  } // if

  // find candidate id and place in selected_value
  if (!header_value.empty() && !url_value.empty()) {
    if (0==header_value.compare(url_value)) {
      selected_value = header_value;
    } else {
      rv.reset(TRI_ERROR_REQUEST_CANCELED,
               "Transaction IDs in URL and headers do not match.");
    } //else
  } else if (!header_value.empty()) {
    selected_value = header_value;
  } else {
    selected_value = url_value;
  } // else

  // is transaction actually valid for this coordinator
  if (!selected_value.empty()) {
    tx_id = selected_value;

    if (transaction::TransactionId::ZERO != tx_id) {
      if (TransactionRegistryFeature::TRANSACTION_REGISTRY->registryId() == tx_id.coordinator) {

        try {
          if (NULL == TransactionRegistryFeature::TRANSACTION_REGISTRY->getInfo(tx_id)) {
            rv.reset(TRI_ERROR_REQUEST_CANCELED,
                     "Transaction is not listed within registry.");
          } // if
        } catch (std::exception const& ex) {
          rv.reset(TRI_ERROR_REQUEST_CANCELED, /*ex.what()*/
                   "Transaction is not listed within registry.");
        } catch (...) {
          rv.reset(TRI_ERROR_INTERNAL);
        } // catch

      } else {
        rv.reset(TRI_ERROR_REQUEST_CANCELED,
                 "Receiving coordinator is not assigned to this transaction.");
      } // else
    } else {
      rv.reset(TRI_ERROR_REQUEST_CANCELED,
               "Transaction ID is not properly formatted.");
    } // else

  } // if

  if (rv.fail()) {
    tx_id.clear();
  } // if

  return rv;
} // RestTransactionHandler::ExtractTransactionId


/// @brief handle the POST methods sent to execute()
void RestTransactionHandler::executePost() {
  bool expectAction(true);

  // identify the type of POST:  multi-operation has "/start" suffix
  if (0!=_request->suffixes().size()) {

    if (1==_request->suffixes().size() && _request->suffixes().at(0).compare("start")) {
      expectAction=false;
    } else {
      generateError(rest::ResponseCode::NOT_FOUND, 404);
      return;
    } // else

  } else {
    // no suffix is single operation transaction
    expectAction=true;
  } // else

  auto slice = _request->payload();
  if(!slice.isObject()){
    generateError(GeneralResponse::responseCode(TRI_ERROR_BAD_PARAMETER),TRI_ERROR_BAD_PARAMETER, "could not acquire v8 context");
    return;
  }

  std::string portType = _request->connectionInfo().portType();

  _v8Context = V8DealerFeature::DEALER->enterContext(_vocbase, true /*allow use database*/);
  if (!_v8Context) {
    generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),TRI_ERROR_INTERNAL, "could not acquire v8 context");
    return;
  }

  TRI_DEFER(returnContext());

  VPackBuilder result;
  try {
    {
      WRITE_LOCKER(lock, _lock);
      if(_canceled){
        generateCanceled();
        return;
      }
    }

    Result res;
    std::tuple<Result, std::string> rvTuple = (*_executeTransactionPtr)(
      _v8Context->_isolate, _lock, _canceled, slice , portType, result, expectAction);

    res = std::get<0>(rvTuple);
    if (!expectAction && res.ok()) {
      _response->setHeader(std::string(kTransactionHeader), std::get<1>(rvTuple));

      VPackObjectBuilder b(&result);
      result.add(kTransactionHeader, VPackValue(std::get<1>(rvTuple)));
    } // if

    if (res.ok()){
      VPackSlice slice = result.slice();
      if (slice.isNone()) {
        generateSuccess(rest::ResponseCode::OK, VPackSlice::nullSlice());
      } else {
        generateSuccess(rest::ResponseCode::OK, slice);
      }
    } else {
      generateError(res);
    }
  } catch (arangodb::basics::Exception const& ex) {
    generateError(GeneralResponse::responseCode(ex.code()),ex.code(), ex.what());
  } catch (std::exception const& ex) {
    generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL), TRI_ERROR_INTERNAL, ex.what());
  } catch (...) {
    generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL), TRI_ERROR_INTERNAL);
  }

  return;
} // RestTransactionHandler::executePost


/// @brief handle the PUT method(s), current only commit
void RestTransactionHandler::executePut() {
  Result res;
  transaction::TransactionId tx_id;

  res=ExtractTransactionId(_request.get(), _response.get(), tx_id);

  if (res.ok()) {
    transaction::Methods * txMethod(nullptr);

    try {
      txMethod = TransactionRegistryFeature::TRANSACTION_REGISTRY->open(tx_id); // throws
      if (nullptr != txMethod) {
        TransactionRegistryFeature::TRANSACTION_REGISTRY->closeCommit(txMethod);
      } else {
        generateError(rest::ResponseCode::NOT_FOUND, 404);
      } // else
    } catch (std::exception const& ex) {
      generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),
                    TRI_ERROR_INTERNAL, ex.what());
    } catch (...) {
      generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),
                    TRI_ERROR_INTERNAL);
    } // catch

  } else {
      generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),
                    TRI_ERROR_INTERNAL, res.errorMessage());
  } // else

  return;
} // RestTransactionHandler::executePut


/// @brief handle the DELETE method(s), current only abort
void RestTransactionHandler::executeDelete() {
  Result res;
  transaction::TransactionId tx_id;

  res=ExtractTransactionId(_request.get(), _response.get(), tx_id);

  if (res.ok()) {
    transaction::Methods * txMethod(nullptr);

    try {
      txMethod = TransactionRegistryFeature::TRANSACTION_REGISTRY->open(tx_id); // throws
      if (nullptr != txMethod) {
        TransactionRegistryFeature::TRANSACTION_REGISTRY->closeAbort(txMethod);
      } else {
        generateError(rest::ResponseCode::NOT_FOUND, 404);
      } // else
    } catch (std::exception const& ex) {
      generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),
                    TRI_ERROR_INTERNAL, ex.what());
    } catch (...) {
      generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),
                    TRI_ERROR_INTERNAL);
    } // catch

  } else {
      generateError(GeneralResponse::responseCode(TRI_ERROR_INTERNAL),
                    TRI_ERROR_INTERNAL, res.errorMessage());
  } // else

  return;
} // RestTransactionHandler::executeDelete
