/*
  If you ever find yourself specifying a new low-level language,
  please include built-in handling of endianness.

  Please.
*/

static unsigned char const big_double[sizeof (double)] =
{
    0xC0, 0x93, 0x4A, 0x00, 0x00, 0x00, 0x00, 0x00
};

static unsigned char const little_double[sizeof (double)] =
{
    0x00, 0x00, 0x00, 0x00, 0x00, 0x4A, 0x93, 0xC0
};

static int endian_double(void)
{
    union {
        double f;
        unsigned char c[sizeof (double)];
    } convert;

    convert.f=-1234.5;
    if (!memcmp(big_double,convert.c,sizeof convert.c))
        return 2;
    if (!memcmp(little_double,convert.c,sizeof convert.c))
        return 1;
    return 0;
}

static unsigned char const big_short[sizeof (unsigned short)] =
{
    0x36, 0x9C
};

static unsigned char const little_short[sizeof (unsigned short)] =
{
    0x9C, 0x36
};

static int endian_short(void)
{
    union {
        unsigned short u;
        unsigned char c[2];
    } convert;

    convert.u=0x369C;
    if (!memcmp(big_short,convert.c,sizeof convert.c))
        return SQLITE_UTF16BE;
    if (!memcmp(little_short,convert.c,sizeof convert.c))
        return SQLITE_UTF16LE;
    return 0;
}

