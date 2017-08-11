////////////////////////////////////////////////////////////////////////////////
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
/// @author Dr. Frank Celler
////////////////////////////////////////////////////////////////////////////////

#ifndef APPLICATION_FEATURES_V8_DEALER_FEATURE_MOCK_H
#define APPLICATION_FEATURES_V8_DEALER_FEATURE_MOCK H 1

#include "V8Server/V8Context.h"
#include "V8Server/V8DealerFeature.h"

namespace arangodb {
namespace tests {

class MockV8DealerFeature : public V8DealerFeature {

 public:
  // Constructor saves current ENGINE global and destructor restores.
  //  This ASSUMES only one MockV8DealerFeature object, or that multiple
  //  destruct in reverse order of construction.  
  MockV8DealerFeature()
    : V8DealerFeature(nullptr)
  {
    _old_global = V8DealerFeature::DEALER;
    V8DealerFeature::DEALER = this;
  }

  ~MockV8DealerFeature() {
    V8DealerFeature::DEALER = _old_global;
  }
  
 protected:
  V8DealerFeature * _old_global;

 public:

  V8Context* enterContext(TRI_vocbase_t*, bool allowUseDatabase,
                          ssize_t forceContext = -1) override {
    return new V8Context(1, nullptr);
  }
    
  void exitContext(V8Context* bogus) override {delete bogus;};

};
}} // two namespaces

#endif
