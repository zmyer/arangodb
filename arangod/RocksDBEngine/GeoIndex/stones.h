/*   Stones header file  stones.h */
/* this is my (R.A.P) test harness for building the GeoIndex */
/* it is also intended as a starting point for implementation */
/* of a "proper" (RocksDB) version.  */

#ifndef ARANGODB_ROCKSDB_GEO_INDEX_STONES_H
#define ARANGODB_ROCKSDB_GEO_INDEX_STONES_H 1

#include <cstddef>
#include <cstdint>

namespace rocksdb {
  class Iterator;
}

struct STON {
  STON(uint64_t oID, size_t kl, size_t vl)
   : objectId(oID), keylength(kl), vallength(vl), totlength(kl + vl) {}
  
  uint64_t objectId;
  size_t const keylength;
  size_t const vallength;
  size_t const totlength;
};

struct GeoLengths {
    size_t keylength;
    size_t vallength;
};

/// wrapper around an index iterator
typedef rocksdb::Iterator SITR;
//struct SITR {
//};                 // yet designed
 
/// index creation parameters
struct GeoParm {
  uint64_t objectId;
};

/// @brief construct a storage system for the keys of a GeoIndex
/// with the given parameters.
STON * StonCons(GeoParm * gs, GeoLengths * gl);

/// destroy (drop) the index with the given GeoParm.
/// There must be no threads connected to it at the time this is
/// executed.
void StonDrop(GeoParm * gs);

/// connect a thread to the specified index.  It is expected that
/// multiple threads will connect to the same index, and different
/// STON structures (which should not be large) will be returned,
/// so that the STON structure is R/W and private to the thread.
STON * StonConnect(GeoParm * gs, GeoLengths * gl);

/// disconnect from the (currently connected) storage system.
/// This does not changed the data in the storage, but frees any
/// memory associated with the STON structure.
void StonDisc(STON * st);

/// insert the batch of key/value pairs into the storage.
/// 0 is returned if successful, and a negative number to
/// indicate an un-recoverable error.  The keyvals string
/// consists of "recs" items, each of which is
/// STON.totlength bytes long.
int StonIns(STON * st, void * trans, uint8_t * keyvals, long recs);

/// delete all the records whose keys are listed.
/// This consists of "recs" items each consisting of
/// STON.keylength bytes.  0 is returned if successful, and a
/// negative number to indicate an unrecoverable error.
int StonDel(STON * st, void * trans, uint8_t * key, long recs);

/// create an iterator (SITR) to read data from the store ready
/// to read keys.  The SITR structure is not read or written by GeoIndex.
/// The trans pointer must be stored in the SITR structure by this routine.
SITR * StonSeek(STON * st, void * trans, uint8_t * key);

/// read data from the iterator up to a maximum of "maxrec" records,
/// and where data higher than maxkey (length STON.keylength) is
/// not really wanted.  "maxrec" is a hard limit, giving the
/// length (in records of length STON.totlength) of the "data"
/// memory available.  "maxkey" is a soft limit, and it is OK to
/// return values higher than this.  If the iterator reads a record
/// it must be returned, so it is likely that there will always be
/// at least one record higher than maxkey unless maxrec is reached.
/// It is also OK to return fewer records - the number read is
/// returned - and indeed initially it may be that only one record
/// is ever returned.  A negative return value indicates an
/// irrecoverable error, so that the transaction must be aborted.
/// A zero return indicates that there are no more records to read.
/// The trans pointer is located in the SITR structure.
size_t StonRead(STON * st, SITR * si, size_t maxrec,
              uint8_t * maxkey, uint8_t * data);

/// destroy the provided SITR and free all associated memory
void StonDestSitr(SITR * si);

/// for testing only
void StonDump(STON * st);

// only for some testings
struct RocksDBGeoV8Functions {
  static void registerResources();
};

#endif
