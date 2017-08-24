#ifndef ARANGOD_AQL_AQL_QUERY_RESULT_CACHE_H
#define ARANGOD_AQL_AQL_QUERY_RESULT_CACHE_H 1

#include "Basics/Common.h"
#include "Basics/Result.h"
#include "Aql/Collection.h"
#include "VocBase/vocbase.h"

#include <velocypack/Slice.h>
#include <velocypack/Builder.h>
#include <velocypack/velocypack-aliases.h>

namespace arangodb {
namespace aql {
namespace cache {

// reads properties into builder
Result properties(VPackBuilder&);
Result properties(VPackSlice const&);
Result clear();

}
}
}

#endif

