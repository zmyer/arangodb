#ifndef ARANGOD_AQL_AQL_QUERY_RESULT_CACHE_H
#define ARANGOD_AQL_AQL_QUERY_RESULT_CACHE_H 1

#include "Basics/Common.h"
#include "Basics/Result.h"
#include "Aql/Collection.h"
#include "VocBase/vocbase.h"

#include <velocypack/Slice.h>
#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

namespace arangodb {
namespace aql {

class AqlItemBlock;
class ExecutionPlan;
class ResourceMonitor;

namespace cache {

//// functions enable/disable, confiugre and invalidate the cache
////  - used in RestHandler and v8-vocbases
// reads properties into builder
Result properties(VPackBuilder&);
// sets properties
Result properties(VPackSlice const&);
// invalidates cache
Result clear();

// generates a hashable represenation of an exectution plan for usage on dbserver
// the empty string signals that the query is not cacheable
std::string fakeQueryString(ExecutionPlan const*);

// convert AqlItemBlock to and VPackSlice contained in passed builder that contains an array arrays of values 
Result blockToVPack(AqlItemBlock const& block, VPackBuilder& builder, std::size_t regs = 0);

auto vPackToBlock(VPackArrayIterator&
                 ,std::unique_ptr<ResourceMonitor>&
                 ,std::size_t atLeast, std::size_t atMost)
  -> std::tuple<Result,std::unique_ptr<AqlItemBlock>, std::size_t>;

}
}
}

#endif
