#include "s3bdformat.h"

unsigned char const s3bd_header_magic[5] =
    "S3BD\x1A";

unsigned char const s3bd_id8_pragmas[7] =
    "pragmas";

unsigned short const s3bd_id16_pragmas[7] =
{
    'p','r','a','g','m','a','s'
};

unsigned char const s3bd_id8_schema[6] =
    "schema";

unsigned short const s3bd_id16_schema[6] =
{
    's','c','h','e','m','a'
};

sqlite3_uint64 const s3bd_uint_bias[9] =
{
                  0x0,
                  0x1,
                0x101,
              0x10101,
            0x1010101,
          0x101010101,
        0x10101010101,
      0x1010101010101,
    0x101010101010101
};

sqlite3_uint64 const s3bd_sint_bias[9] =
{
                  0x0,
                  0x1,
                 0x81,
               0x8081,
             0x808081,
           0x80808081,
         0x8080808081,
       0x808080808081,
     0x80808080808081
};

