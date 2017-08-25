/*   Stones module  stones.c */
/* this is my (R.A.P) test harness for building the GeoIndex */
/* it is aimed at providing the functionality with minimum effort */
/* and maximum clarity . . . it must work, and its performance is */
/* of no interest.  It will eventually be discarded */
/*   Famous last words!  */

// uncomment next line for stones test;  comment for use
#define MAINHERE 1

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "stones.h"


/* The "database" - held in global data */
uint8_t * StonesData;
long StonesRecs;

/* NOTE - no realloc in "stones" at the moment!  */
/* make this big enough!                         */
/* this is the number of records it can store    */

#define MAXSIZE 1000
long StonesAlloc;       // so this is set but not used */

STON * StonCons(GeoParm * gs, GeoLengths * gl)
{
    STON * st;
    st=malloc(sizeof(STON));
    st->keylength=gl->keylength;
    st->vallength=gl->vallength;
    st->totlength=st->keylength+st->vallength;
    StonesRecs=0;
    StonesAlloc=MAXSIZE;
    StonesData=malloc(StonesAlloc*st->totlength);
    return st;
}

void StonDrop(GeoParm * gs)
{
    free(StonesData);
}

STON * StonConnect(GeoParm * gs, GeoLengths * gl)
{
    STON * st;
    st=malloc(sizeof(STON));
    st->keylength=gl->keylength;   // this had better agree!
    st->vallength=gl->vallength;
    st->totlength=st->keylength+st->vallength;
}

void StonDisc(STON * st)
{
    free(st);
}

// Full table scan!  slow but should work

long StonFind(STON * st, uint8_t * key)
{
    long ix;
    int j;
    for(ix=0;ix<StonesRecs;ix++)
    {
        j=memcmp(StonesData+ix*st->totlength,key,st->keylength); 
        if(j>=0) break;
    }
    return ix;
}

long StonIns(STON * st, void * trans, uint8_t * keyvals, long recs)
{
    long i,ix;
    for(i=0;i<recs;i++)
    {
        ix=StonFind(st,keyvals+i*st->totlength);
        memmove(StonesData+(ix+1)*st->totlength,
                StonesData+ix*st->totlength,
                (StonesRecs-ix)*st->totlength);
        memcpy(StonesData+ix*st->totlength,
               keyvals+i*st->totlength,
               st->totlength);
        StonesRecs++;
    }
    return 0;
}

long StonDel(STON * st, void * trans, uint8_t * key, long recs)
{
    long i,ix;
    int j;
    for(i=0;i<recs;i++)
    {
        ix=StonFind(st,key+i*st->keylength);
        if(ix==StonesRecs) return -1;
        j=memcmp(StonesData+ix*st->totlength,
                 key+i*st->keylength,
                 st->keylength);
        if(j!=0) return -1;        
        memmove(StonesData+ix*st->totlength,
                StonesData+(ix+1)*st->totlength,
                (StonesRecs-ix-1)*st->totlength);
        StonesRecs--;
    }
    return 0;
}

SITR * StonSeek(STON * st, void * trans, uint8_t * key)
{
    SITR * si;
    si=malloc(sizeof(SITR));
    si->nextkey=malloc(st->keylength);
    si->trans=trans;                     // not used though
    memcpy(si->nextkey,key,st->keylength);
    return si;
}

long StonRead(STON * st, SITR * si, long maxrec, 
              uint8_t * maxkey, uint8_t * data)
{
    long i,ix;
    int j;
    for(i=0;i<maxrec;i++)
    {
        ix=StonFind(st,si->nextkey);
        if(ix>=StonesRecs) break;
        memcpy(data+i*st->totlength,
               StonesData+ix*st->totlength,
               st->totlength);
        memcpy(si->nextkey,
               StonesData+ix*st->totlength,
               st->keylength);
        for(j=0;j<st->keylength;j++)
        {
            si->nextkey[st->keylength-j-1]++;
            if(si->nextkey[st->keylength-j-1]!=0) break;
        }
        j=memcmp(StonesData+ix*st->totlength,
                 maxkey,
                 st->keylength);
        if(j>0) return i+1;
    }
    return i;
}

void StonDestSitr(SITR * si)
{
    free(si->nextkey);
    free(si);
}

void hexput(uint8_t c)
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

void StonDump(STON * st)
{
    int i,j;
    printf("Dump of database %ld records\n",StonesRecs);
    for(i=0;i<StonesRecs;i++)
    {
        for(j=0;j<st->keylength;j++) 
            hexput(*(StonesData+i*st->totlength+j));
        printf("-- ");
        for(;j<st->totlength;j++) 
            hexput(*(StonesData+i*st->totlength+j));
        printf("\n");
    }
    printf("\n");
}

#ifdef MAINHERE

void ResDump(STON * st,uint8_t * res,long recs)
{
    int i,j;
    printf("Dump of results %ld records\n",recs);
    for(i=0;i<recs;i++)
    {
        for(j=0;j<st->keylength;j++) 
            putchar(*(res+i*st->totlength+j));
        printf(" ");
        for(;j<st->totlength;j++) 
            putchar(*(res+i*st->totlength+j));
        printf("\n");
    }
    printf("\n");
}

uint8_t tt[1000];
long ttl;
uint8_t kk[1000];
long kkl;

void addlist(STON * st, char *k, char *v)
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

void addkey(STON * st, char *k)
{
    uint8_t ky[20];
    memset(ky,'-',20);
    memcpy(ky,k,strlen(k));
    memcpy(kk+kkl*st->keylength,ky,st->keylength);
    kkl++;
}

void * StonesStartTrans()
{
    return NULL;
}

void StonesCommit(void * trans)
{
    return;
}

void * StonesStartRTrans()
{
    return NULL;
}

void StonesEndRTrans(void * trans)
{
    return;
}

int main(int argc, char ** argv)
{
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
    addlist(st,"bing","bong");
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
    si=StonSeek(st,trans,"cat------");
    r=StonRead(st,si,9,"fat------",dd);
    ResDump(st,dd,r);
    StonDestSitr(si);
    si=StonSeek(st,trans,"cat------");
    r=StonRead(st,si,1,"fat------",dd);
    StonesEndRTrans(trans);
    ResDump(st,dd,r);
    StonDestSitr(si);
    StonDisc(st);
    StonDrop(&gs);
    printf("Stones test completed\n");
}

#endif

/* end of stones.c  */
