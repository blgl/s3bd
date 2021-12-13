#ifndef S3BDFORMAT_H
#define S3BDFORMAT_H

/*
  Steal this if you need to create an independent reimplementation.

  To be read in parallel with format.txt.
*/

#include <sqlite3.h>

typedef struct s3bd_header_t {
    unsigned char magic[5];
    unsigned char ver_major;
    unsigned char ver_minor;
    unsigned char encoding;
} s3bd_header_t;

#define CURVER_MAJOR	0
#define CURVER_MINOR	0

extern unsigned char const s3bd_header_magic[5];

extern unsigned char const s3bd_id8_pragmas[7];

extern unsigned short const s3bd_id16_pragmas[7];

#define PRAGMA_PHASE_PRE_TRANSACTION	10
#define PRAGMA_PHASE_IN_TRANSACTION	20
#define PRAGMA_PHASE_POST_TRANSACTION	30

extern unsigned char const s3bd_id8_schema[6];

extern unsigned short const s3bd_id16_schema[6];

#define SCHEMA_PHASE_TABLE		10
#define SCHEMA_PHASE_INDEX		20
#define SCHEMA_PHASE_VIRTUAL_TABLE	30
#define SCHEMA_PHASE_VIEW		40
#define SCHEMA_PHASE_TRIGGER		50

#define BASE9(a,b,c)	(((a)*9+(b))*9+(c))

#define NULLCOL()	BASE9(0,0,0)
#define ENDSET()	BASE9(0,0,1)
#define ENDDUMP()	BASE9(0,0,2)

#define INTCOL(iw)	BASE9(1,0,iw)
#define FLOATCOL(fw)	BASE9(1,1,fw)
#define TEXTCOL(tsw)	BASE9(1,2,tsw)
#define BLOBCOL(bsw)	BASE9(1,3,bsw)

#define ROWSET(ccw,nsw) BASE9(2,ccw,nsw)

#define is_NULLCOL(m)	((m)==NULLCOL())
#define is_ENDSET(m)	((m)==ENDSET())
#define is_ENDDUMP(m)	((m)==ENDDUMP())

#define is_INTCOL(m)	((m)>=INTCOL(0) && (m)<=INTCOL(8))
#define is_FLOATCOL(m)	((m)>=FLOATCOL(0) && (m)<=FLOATCOL(8))
#define is_TEXTCOL(m)	((m)>=TEXTCOL(0) && (m)<=TEXTCOL(8))
#define is_BLOBCOL(m)	((m)>=BLOBCOL(0) && (m)<=BLOBCOL(8))

#define is_ROWSET(m)	((m)>=ROWSET(0,0) && (m)<=ROWSET(8,8))

#define INTCOL_iw(m)	((m)%9)
#define FLOATCOL_fw(m)	((m)%9)
#define TEXTCOL_tsw(m)	((m)%9)
#define BLOBCOL_bsw(m)	((m)%9)

#define ROWSET_ccw(m)	((m)/9%9)
#define ROWSET_nsw(m)	((m)%9)

extern sqlite3_uint64 const s3bd_uint_bias[9];

extern sqlite3_uint64 const s3bd_sint_bias[9];

#endif /* S3BD_FORMAT_H */

