/*
** 2017-12-17
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** Utility functions sqlar_compress() and sqlar_uncompress(). Useful
** for working with sqlar archives and used by the shell tool's built-in
** sqlar support.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#undef COMPRESS_ZLIB /* define this to use zlib instead of zstd */
#ifdef SQLITE_HAVE_ZLIB
#include <zlib.h>
#endif
#ifdef SQLITE_HAVE_ZSTD
#include <zstd.h>
#endif
#include <assert.h>

/*
** Implementation of the "sqlar_compress(X,#)" SQL function.
**
** If the type of X is SQLITE_BLOB, and compressing that blob using
** zlib utility function compress() yields a smaller blob, return the
** compressed blob. Otherwise, return a copy of X.
**
** SQLar uses the "zlib format" for compressed content.  The zlib format
** contains a two-byte identification header and a four-byte checksum at
** the end.  This is different from ZIP which uses the raw deflate format.
** The "zstd format" uses four-byte magic, and a four-byte XXH64 checksum.
** It also allows compression levels higher than 9, like 19 or 22 (ultra).
** The decompression speed is better, and fast for all compression levels.
**
** Future enhancements to SQLar might add support for new compression formats.
** If so, those new formats will be identified by alternative headers in the
** compressed data.
**
** zlib magic bytes: 78 5e (fast) | 78 9c (default) | 78 7d (best)
** zstd magic bytes: 28 b5 2f fd (same for all compression levels)
*/
#define MAGIC_ZLIB_0 0x78
#define MAGIC_ZSTD_0 0x28
static void sqlarCompressFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( argc==2 );
  if( sqlite3_value_type(argv[0])==SQLITE_BLOB ){
    const Bytef *pData = sqlite3_value_blob(argv[0]);
    uLong nData = sqlite3_value_bytes(argv[0]);
    int lvl = sqlite3_value_int(argv[1]);
#ifdef COMPRESS_ZLIB
    uLongf nOut = compressBound(nData);
    Bytef *pOut;
    if( lvl == -1 )
      lvl = Z_DEFAULT_COMPRESSION;
    else if( lvl > Z_BEST_COMPRESSION )
      lvl = Z_BEST_COMPRESSION;
#else
    size_t nOut = ZSTD_compressBound(nData);
    void *pOut;
    if( lvl == -1 )
      lvl = ZSTD_CLEVEL_DEFAULT;
    else if( lvl > ZSTD_maxCLevel() )
      lvl = ZSTD_maxCLevel();
#endif

    pOut = (Bytef*)sqlite3_malloc(nOut);
    if( pOut==0 ){
      sqlite3_result_error_nomem(context);
      return;
    }else{
#ifdef COMPRESS_ZLIB
      if( Z_OK!=compress2(pOut, &nOut, pData, nData, lvl) ){
#else
      if( ZSTD_isError(nOut = ZSTD_compress(pOut, nOut, pData, nData, lvl)) ){
#endif
        sqlite3_result_error(context, "error in compress()", -1);
      }else if( nOut<nData && lvl != 0 ){
        sqlite3_result_blob(context, pOut, nOut, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_value(context, argv[0]);
      }
      sqlite3_free(pOut);
    }
  }else{
    sqlite3_result_value(context, argv[0]);
  }
}

/*
** Implementation of the "sqlar_uncompress(X,SZ)" SQL function
**
** Parameter SZ is interpreted as an integer. If it is less than or
** equal to zero, then this function returns a copy of X. Or, if
** SZ is equal to the size of X when interpreted as a blob, also
** return a copy of X. Otherwise, decompress blob X using zlib
** utility function uncompress() and return the results (another
** blob).
*/
static void sqlarUncompressFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  uLong nData;
  sqlite3_int64 sz;

  assert( argc==2 );
  sz = sqlite3_value_int(argv[1]);

  if( sz<=0 || sz==(nData = sqlite3_value_bytes(argv[0])) ){
    sqlite3_result_value(context, argv[0]);
  }else{
    uLongf szf = sz;
    const Bytef *pData= sqlite3_value_blob(argv[0]);
    Bytef *pOut = sqlite3_malloc(sz);
    if( pOut==0 ){
      sqlite3_result_error_nomem(context);
#ifdef COMPRESS_ZLIB
    }else if( nData<=2 || pData[0] != MAGIC_ZLIB_0 ){
      /* not in zlib format, copy as-is */
      sqlite3_result_value(context, argv[0]);
    }else if( Z_OK!=uncompress(pOut, &szf, pData, nData) ){
#else
    }else if( nData<=4 || pData[0] != MAGIC_ZSTD_0 ){
      /* not in zstd format, copy as-is */
      sqlite3_result_value(context, argv[0]);
    }else if( ZSTD_isError(szf = ZSTD_decompress(pOut, szf, pData, nData)) ){
#endif
      sqlite3_result_error(context, "error in uncompress()", -1);
    }else{
      sqlite3_result_blob(context, pOut, szf, SQLITE_TRANSIENT);
    }
    sqlite3_free(pOut);
  }
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sqlar_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "sqlar_compress", 2,
                               SQLITE_UTF8|SQLITE_INNOCUOUS, 0,
                               sqlarCompressFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "sqlar_uncompress", 2,
                                 SQLITE_UTF8|SQLITE_INNOCUOUS, 0,
                                 sqlarUncompressFunc, 0, 0);
  }
  return rc;
}
