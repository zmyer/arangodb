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
/// @author Jan Steemann
////////////////////////////////////////////////////////////////////////////////

#include "types.h"

using namespace arangodb::transaction;

const TransactionId TransactionId::ZERO = TransactionId(0,0);

TransactionId::TransactionId(std::string & stringId) {
  *this = stringId;
}

TransactionId::TransactionId(const char * charId) {
  if (nullptr != charId) {
    std::string temp(charId);
    operator=(temp);
  } // if
}

TransactionId & TransactionId::operator=(std::string & stringId) {

  auto sep_pos = stringId.find_first_not_of("0123456789");
  *this = ZERO;

  if (std::string::npos != sep_pos && 0 != sep_pos
      && SEPARATOR == stringId.at(sep_pos)) {
    coordinator = std::stoull(stringId);

    ++sep_pos;
    if (sep_pos<stringId.length()
        && std::string::npos == stringId.find_first_not_of("0123456789", sep_pos)) {
      identifier = std::stoull(stringId.substr(sep_pos));
    } else {
      *this = ZERO;
    } // else
  } // if

  return *this;
} // TransactionId::operator=(std::string&)


TransactionId & TransactionId::operator=(char const * charId) {
  if (nullptr != charId) {
    std::string temp(charId);
    operator=(temp);
  } // if

  return *this;
} // TransactionId::operator=(char const *)
