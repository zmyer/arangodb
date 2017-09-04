/*   Stones module  stones.c */
/* this is my (R.A.P) test harness for building the GeoIndex */
/* it is aimed at providing the functionality with minimum effort */
/* and maximum clarity . . . it must work, and its performance is */
/* of no interest.  It will eventually be discarded */
/*   Famous last words!  */

// uncomment next line for stones test;  comment for use
#define MAINHERE 1

#include "stones.h"
#include "Basics/Common.h"
#include "Logger/Logger.h"
#include "Transaction/Methods.h"
#include "RocksDBEngine/RocksDBColumnFamily.h"
#include "RocksDBEngine/RocksDBKey.h"
#include "RocksDBEngine/RocksDBMethods.h"
#include "RocksDBEngine/RocksDBTransactionState.h"

#include <stdlib.h>

/// ========= demo stuff ===========
#include "V8/v8-globals.h"
#include "V8/v8-utils.h"
#include "V8/v8-conv.h"
#include "V8Server/v8-externals.h"
/// ================================

using namespace arangodb;

STON * StonCons(GeoParm * gs, GeoLengths * gl) {
  return new STON(gs->objectId, gl->keylength, gl->vallength);
}

void StonDrop(GeoParm * gs) {
  // basically do nothing, this should be handled outside
}

STON * StonConnect(GeoParm * gs, GeoLengths * gl) {
    return new STON(gs->objectId, gl->keylength, gl->vallength);
}

void StonDisc(STON * st) {
  TRI_ASSERT(st != nullptr);
  delete st;
}

int StonIns(STON * st, void * trans, uint8_t * keyvals, long recs) {
  if (trans != nullptr) {
    transaction::Methods* trx = reinterpret_cast<transaction::Methods*>(trans);
    RocksDBMethods *mthds = RocksDBTransactionState::toMethods(trx);
    RocksDBKeyLeaser key(trx);
    for (long ix = 0; ix < recs; ix++) {
      char* kk = reinterpret_cast<char*>(keyvals + ix * st->totlength);
      char* vv = reinterpret_cast<char*>(keyvals + ix * st->totlength + st->keylength);
      key->constructPrimaryIndexValue(st->objectId, StringRef(kk, st->keylength));
      mthds->Put(RocksDBColumnFamily::geo(), key.ref(),
                                     rocksdb::Slice(vv, st->vallength));
    }
  } else {
    RocksDBKey key;
    rocksdb::TransactionDB* db = rocksutils::globalRocksDB();
    rocksdb::WriteOptions wo;
    for (long ix = 0; ix < recs; ix++) {
      char* kk = reinterpret_cast<char*>(keyvals + ix * st->totlength);
      char* vv = reinterpret_cast<char*>(keyvals + ix * st->totlength + st->keylength);
      key.constructPrimaryIndexValue(st->objectId, StringRef(kk, st->keylength));
      rocksdb::Status s = db->Put(wo, RocksDBColumnFamily::geo(), key.string(),
                                  rocksdb::Slice(vv, st->vallength));
      if (!s.ok()) {
        return - s.code();
      }
    }
  }
  return TRI_ERROR_NO_ERROR;
}

int StonDel(STON * st, void * trans, uint8_t * keys, long recs) {
  if (trans != nullptr) {
    transaction::Methods* trx = reinterpret_cast<transaction::Methods*>(trans);
    RocksDBMethods *mthds = RocksDBTransactionState::toMethods(trx);
    RocksDBKeyLeaser key(trx);
    for (long ix = 0; ix < recs; ix++) {
      char* kk = reinterpret_cast<char*>(keys + ix * st->totlength);
      key->constructPrimaryIndexValue(st->objectId, StringRef(kk, st->keylength));
      mthds->Delete(RocksDBColumnFamily::geo(), key.ref());
    }
  } else {
    RocksDBKey key;
    rocksdb::TransactionDB* db = rocksutils::globalRocksDB();
    rocksdb::WriteOptions wo;
    for (long ix = 0; ix < recs; ix++) {
      char* kk = reinterpret_cast<char*>(keys + ix * st->totlength);
      key.constructPrimaryIndexValue(st->objectId, StringRef(kk, st->keylength));
      rocksdb::Status s = db->Delete(wo, RocksDBColumnFamily::geo(), key.string());
      if (!s.ok()) {
        return - s.code();
      }
    }
  }
  return TRI_ERROR_NO_ERROR;
}

SITR * StonSeek(STON * st, void * trans, uint8_t * key) {
  std::unique_ptr<rocksdb::Iterator> iter;
  if (trans != nullptr) {
    transaction::Methods* trx = reinterpret_cast<transaction::Methods*>(trans);
    RocksDBMethods *mthds = RocksDBTransactionState::toMethods(trx);
    iter = mthds->NewIterator(RocksDBColumnFamily::geo());
  } else {
    rocksdb::ReadOptions ro;
    iter.reset(rocksutils::globalRocksDB()->NewIterator(ro));
  }
  iter->Seek(rocksdb::Slice(reinterpret_cast<char*>(key), st->keylength));
  return iter.release();// SITR is a typedef for rocksdb::Iterator
}

size_t StonRead(STON * st, SITR * si, size_t maxrec,
              uint8_t * maxkey, uint8_t * data) {
  size_t numRecords = 0;
  rocksdb::Iterator* it = si;
  while (it->Valid() && numRecords < maxrec) {
    StringRef key = RocksDBKey::vertexId(it->key());
    TRI_ASSERT(key.length() == st->keylength);
    uint8_t* keyval = data + numRecords * st->totlength;
    memcpy(keyval, key.data(), key.length());
    TRI_ASSERT(it->value().size() == st->vallength);
    memcpy(keyval + st->keylength, it->value().data(), it->value().size());
    numRecords++;
    // return at most one more key
    if (memcmp(key.data(), maxkey, key.length()) > 0) {
      break;
    }
    it->Next();
  }
  return numRecords;
}

void StonDestSitr(SITR * si) {
  rocksdb::Iterator* it = si;
  delete it;
}

void hexput(char c)
{
    uint8_t d;
    d=(c>>4)&15;
    if(d<=9) printf("%c",d+'0');
      else   printf("%c",d+'A'-10);
    d=c&15;
    if(d<=9) printf("%c",d+'0');
      else   printf("%c",d+'A'-10);
    printf(" ");
}

void StonDump(STON * st) {
  RocksDBKeyBounds bound = RocksDBKeyBounds::PrimaryIndex(st->objectId);
  rocksdb::ReadOptions ro;
  rocksdb::Slice end = bound.end();
  ro.iterate_upper_bound = &end;
  ro.readahead_size = 2 * 1024 * 1024;
  ro.verify_checksums = true;
  ro.fill_cache = false;
  ro.prefix_same_as_start = true;
  
  std::unique_ptr<rocksdb::Iterator> it(rocksutils::globalRocksDB()->NewIterator(ro));
  for (it->Seek(bound.start()); it->Valid(); it->Next()) {
    
    StringRef key = RocksDBKey::vertexId(it->key());
    TRI_ASSERT(key.length() == st->keylength);
    for(size_t j= 0; j < st->keylength; j++) {
      hexput(key.at(j));
    }
    
    TRI_ASSERT(it->value().size() == st->vallength);
    for(size_t j= 0; j < st->vallength; j++) {
      hexput(*(it->value().data() + j));
    }
  }
}

#ifdef MAINHERE

void ResDump(STON * st,uint8_t * res, size_t recs)
{
    LOG_TOPIC(INFO, Logger::DEVEL) <<  "Dump of results " << recs << " records";
    for(size_t i=0;i<recs;i++) {
        for(size_t j=0;j<st->keylength;j++)
            putchar(*(res+i*st->totlength+j));
        printf(" ");
        for(size_t j = 0; j < st->totlength;j++)
            putchar(*(res+i*st->totlength+j));
        printf("\n");
    }
    printf("\n");
}

uint8_t tt[1000];
long ttl;
uint8_t kk[1000];
long kkl;

void addlist(STON * st, char const* k, char const* v)
{
    uint8_t ky[20];
    uint8_t vl[20];
    memset(ky,'-',20);
    memset(vl,'-',20);
    memcpy(ky,k,strlen(k));
    memcpy(vl,v,strlen(v));
    memcpy(tt+ttl*st->totlength,ky,st->keylength);
    memcpy(tt+ttl*st->totlength+st->keylength,vl,st->vallength);
    ttl++;
}

void addkey(STON * st, char const* k)
{
    uint8_t ky[20];
    memset(ky,'-',20);
    memcpy(ky,k,strlen(k));
    memcpy(kk+kkl*st->keylength,ky,st->keylength);
    kkl++;
}

void * StonesStartTrans() {
    return NULL;
}

void StonesCommit(void * trans) {
    return;
}

void * StonesStartRTrans() {
    return NULL;
}

void StonesEndRTrans(void * trans) {
    return;
}

void JS_StonesTest(v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  
  // BEGIN richard test
    STON * st;
    SITR * si;
    void * trans;
    long r;
    uint8_t dd[200];
    GeoParm gs;
    GeoLengths gl;
    gl.keylength=9;
    gl.vallength=11;
    st=StonCons(&gs,&gl);
    StonDisc(st);        // just so it has been executed
    st=StonConnect(&gs,&gl);
    ttl=0;
    addlist(st,"bing", "bong");
    addlist(st,"crumble","cherry");
    addlist(st,"agammemnon","aspidistra");
    trans=StonesStartTrans();
    StonIns(st,trans,tt,ttl);
    StonesCommit(trans);
    StonDump(st);
    kkl=0;
    addkey(st,"bing");
    trans=StonesStartTrans(); 
    StonDel(st,trans,kk,kkl);
    StonesCommit(trans);
    StonDump(st);
    ttl=0;
    addlist(st,"bang","bong");
    addlist(st,"drig","dorg");
    addlist(st,"eeee","eee1");
    addlist(st,"fogg","find");
    addlist(st,"greg","grog");
    trans=StonesStartTrans();
    StonIns(st,trans,tt,ttl);
    StonesCommit(trans);
    StonDump(st);
    trans=StonesStartRTrans();
    si=StonSeek(st, trans, (uint8_t*)"cat------");
    r=StonRead(st,si,9, (uint8_t*)"fat------",dd);
    ResDump(st,dd,r);
    StonDestSitr(si);
    si=StonSeek(st,trans, (uint8_t*)"cat------");
    r=StonRead(st,si,1, (uint8_t*)"fat------",dd);
    StonesEndRTrans(trans);
    ResDump(st,dd,r);
    StonDestSitr(si);
    StonDisc(st);
    StonDrop(&gs);
  // END richard test
  LOG_TOPIC(INFO, Logger::DEVEL) << "Stones test completed";
  
  TRI_V8_TRY_CATCH_END
}

#include <cstdlib>

void JS_StonesBenchmark(v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  
  if (args.Length() < 3 || !args[0]->IsNumber() ||
      !args[1]->IsNumber() || !args[2]->IsNumber()) {
    TRI_V8_THROW_EXCEPTION_USAGE("STONES_BENCHMARK(t, q, c)");
  }
  uint32_t t = (uint32_t) TRI_ObjectToUInt64(args[0], true);
  uint32_t q = (uint32_t) TRI_ObjectToUInt64(args[1], true);
  uint32_t c = (uint32_t) TRI_ObjectToUInt64(args[2], true);
  
  GeoParm gs = {.objectId = 1};
  GeoLengths gl;
  gl.keylength = 9;
  gl.vallength = 11;
  STON * st = StonCons(&gs, &gl);
  void * trans = nullptr;

  char keyBuf[st->keylength];
  char valBuf[st->vallength];
  for (uint32_t i = 0; i < t; i += 5) {
    ttl=0;
    for (uint32_t j = i; j < i + 5; j++) {
      memset(keyBuf, '-', st->keylength);
      memset(valBuf, '-', st->vallength);
      std::sprintf(keyBuf,"%x",j);
      std::sprintf(valBuf,"%x",j);
      addlist(st, keyBuf, valBuf);
    }
    StonIns(st,trans,tt,ttl);
  }
  
  srand(time(NULL));
  
  // create c cursors at random points
  SITR* cursors[c];
  for (size_t i = 0; i < c; i++) {
    int n = rand() % t;
    memset(keyBuf, '-', st->keylength);
    std::sprintf(keyBuf, "%x", n);
    cursors[i] = StonSeek(st, trans, (uint8_t*)keyBuf);
  }
  
  char maxKey[st->keylength];
  std::sprintf(maxKey,"%x", t);
  
  int64_t numRead = std::max((uint32_t)1, q / c);
  do {
    // read one document from each cursor
    for (size_t i = 0; i < c; i++) {
      uint8_t keyVal[st->totlength * 2];
      size_t c = StonRead(st, cursors[i], 1, (uint8_t*)maxKey, keyVal);
      if (c != 1) {
        LOG_TOPIC(INFO, Logger::DEVEL) << "Error reading value from rocksdb";
        TRI_V8_RETURN_UNDEFINED();;
      }
    }
  } while(--numRead > 0);
  
  TRI_V8_TRY_CATCH_END
}

void RocksDBGeoV8Functions::registerResources() {
  ISOLATE;
  v8::HandleScope scope(isolate);
  
  // add global testing functions
  TRI_AddGlobalFunctionVocbase(isolate, TRI_V8_ASCII_STRING("STONES_TEST"),
                               JS_StonesTest, true);

  TRI_AddGlobalFunctionVocbase(isolate, TRI_V8_ASCII_STRING("STONES_BENCHMARK"),
                               JS_StonesBenchmark, true);
}

#endif

/* end of stones.c  */
