/*   Stones header file  stones.h */
/* this is my (R.A.P) test harness for building the GeoIndex */
/* it is also intended as a starting point for implementation */
/* of a "proper" (RocksDB) version.  */

typedef struct
{
    size_t keylength;
    size_t vallength;
    size_t totlength;
} STON;

typedef struct
{
    size_t keylength;
    size_t vallength;
} GeoLengths;

typedef struct
{
    void * trans;
    uint8_t * nextkey;
} SITR;                 // yet designed

typedef struct
{
    int boggle;    // the test harness does not use this at all
} GeoParm;         // as it only deals with one index

STON * StonCons(GeoParm * gs, GeoLengths * gl);
void StonDrop(GeoParm * gs);
STON * StonConnect(GeoParm * gs, GeoLengths * gl);
void StonDisc(STON * st);
long StonIns(STON * st, void * trans, uint8_t * keyvals, long recs);
long StonDel(STON * st, void * trans, uint8_t * key, long recs);
SITR * StonSeek(STON * st, void * trans, uint8_t * key);
long StonRead(STON * st, SITR * si, long maxrec, 
              uint8_t * maxkey, uint8_t * data);
void StonDestSitr(SITR * si);

void StonDump(STON * st);   // for testing only;

/* end of stones.h  */
