////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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
/// @author Kaveh Vahedipour
/// @author Dr. Frank Celler
////////////////////////////////////////////////////////////////////////////////

#include "TransactionRegistryFeature.h"

#include "Transaction/Methods.h"
#include "Transaction/TransactionRegistry.h"
#include "ProgramOptions/ProgramOptions.h"
#include "ProgramOptions/Section.h"

using namespace arangodb;
using namespace arangodb::application_features;
using namespace arangodb::basics;
using namespace arangodb::options;

transaction::TransactionRegistry* TransactionRegistryFeature::TRANSACTION_REGISTRY = nullptr;

TransactionRegistryFeature::TransactionRegistryFeature(ApplicationServer* server)
    : ApplicationFeature(server, "TransactionRegistry"),
      _transactionTracking(true),
      _failOnWarning(false),
      _transactionMemoryLimit(0),
      _slowThreshold(10.0) {
  setOptional(false);
  requiresElevatedPrivileges(false);
  startsAfter("DatabasePath");
  startsAfter("Database");
  startsAfter("MMFilesLogfileManager");
  startsAfter("Cluster");
}

void TransactionRegistryFeature::collectOptions(
    std::shared_ptr<ProgramOptions> options) {
  options->addSection("transaction", "Configure transactions");
  
  options->addOption("--transaction.memory-limit", "memory threshold for transactions (in bytes)",
                     new UInt64Parameter(&_transactionMemoryLimit));

  options->addOption("--transaction.tracking", "whether to track slow transactions",
                     new BooleanParameter(&_transactionTracking));
  
  options->addOption("--transaction.fail-on-warning", "whether transactions should fail with errors even for recoverable warnings",
                     new BooleanParameter(&_failOnWarning));
  
  options->addOption("--transaction.slow-threshold", "threshold for slow transactions (in seconds)",
                     new DoubleParameter(&_slowThreshold));

}

void TransactionRegistryFeature::prepare() {
  
  // create the transaction registery
  _transactionRegistry.reset(new transaction::TransactionRegistry());
  TRANSACTION_REGISTRY = _transactionRegistry.get();
  
}

void TransactionRegistryFeature::start() {}

void TransactionRegistryFeature::unprepare() {
  // clear the transaction registery
  TRANSACTION_REGISTRY = nullptr;
}
