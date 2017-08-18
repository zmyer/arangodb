////////////////////////////////////////////////////////////////////////////////
/// @brief test cases for types.h
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

#include "Transaction/types.h"

using namespace arangodb;

namespace arangodb {
namespace tests {
namespace types_test {


TEST_CASE("TransactionId tests", "[unittest]") {

  SECTION("Valid initializations") {
    transaction::TransactionId new_id;

    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );
    REQUIRE( 0==transaction::TransactionId::ZERO.coordinator);
    REQUIRE( 0==transaction::TransactionId::ZERO.identifier );
    REQUIRE( transaction::TransactionId::ZERO==new_id);

    new_id="42-86";
    REQUIRE( 42==new_id.coordinator );
    REQUIRE( 86==new_id.identifier );
    REQUIRE( new_id != transaction::TransactionId::ZERO );

    std::string temp_str("12309-32123");
    new_id=temp_str;
    REQUIRE( 12309==new_id.coordinator );
    REQUIRE( 32123==new_id.identifier );

    temp_str="-23432";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="3fred4-23432";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="32343fred-23432";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="32343-sam23432";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="32343-23422boss";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="32343-";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="-";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="8987";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    temp_str="";
    new_id=temp_str;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    new_id=nullptr;
    REQUIRE( 0==new_id.coordinator );
    REQUIRE( 0==new_id.identifier );

    transaction::TransactionId id_two("866-12345");
    REQUIRE( 866==id_two.coordinator );
    REQUIRE( 12345==id_two.identifier );

    temp_str = "3493493-283992983";
    transaction::TransactionId id_three(temp_str);
    REQUIRE( 3493493==id_three.coordinator );
    REQUIRE( 283992983==id_three.identifier );

    REQUIRE( id_two != id_three );
    id_two=id_three;
    REQUIRE( id_two == id_three );
    REQUIRE( 3493493==id_two.coordinator );
    REQUIRE( 283992983==id_two.identifier );


  } // SECTION

  SECTION("Original TX: succeeds") {
    transaction::TransactionId new_id;

  } // SECTION
} // TEST_CASE

}}} // three namespaces
