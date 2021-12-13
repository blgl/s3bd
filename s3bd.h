#ifndef S3BD_H
#define S3BD_H

#include <stdio.h>
#include <sqlite3.h>

/*
  s3bd_store writes a dump file and returns an SQLite3 status code.
  Optionally returns an error message string (caller must sqlite3_free).
  Assumes that the names "pragmas" and "schema" are available for use
  in the temp database.

  S3BD_STORE_SCHEMA_ONLY means to omit the actual table contents.

  S3BD_STORE_IN_TRANSACTION means to assume a transaction is already active.
  The intended purpose is:
    1. begin a transaction
    2. drop/delete data you don't want to include in the dump
    3. call s3bd_store
    4. rollback the transaction

  The list of pragma overrides must be terminated by a NULL pointer.
  Each string in the list must look like either "name=value" to replace
  a pragma value or just "name" to omit it.  Unknown names are ignored;
  you can't add new pragmas.
*/

#define S3BD_STORE_SCHEMA_ONLY		0x1
#define S3BD_STORE_IN_TRANSACTION	0x2

extern int s3bd_store(
    sqlite3 *connection,
    FILE *outfile,
    unsigned int flags,
    char const * const *overrides,
    char **errmsg);


/*
  s3bd_load reads a dump file and returns an SQLite3 status code.
  Optionally returns an error message string (caller must sqlite3_free).
  The destination database must be freshly created and untouched.

  S3BD_LOAD_SCHEMA_ONLY means to omit the actual table contents.

  The list of pragma overrides must be terminated by a NULL pointer.
  Each string in the list must look like either "name=value" to replace
  a pragma value or just "name" to omit it.  Unknown names are ignored;
  you can't add new pragmas.
*/

#define S3BD_LOAD_SCHEMA_ONLY		0x1

extern int s3bd_load(
    sqlite3 *connection,
    FILE *infile,
    unsigned int flags,
    char const * const *overrides,
    char **errmsg);

#endif

