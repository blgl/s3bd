/*
  Your basic transparent append-only string buffer, used to cobble up
  dynamic SQL code when bound parameters are insufficient.
  Most operations come in both 8- and 16-bit versions.
  (SQLite's own sqlite3_str only does 8-bit.)

  *app_7        append an ASCII string (8-bit source even in 16-bit mode)
  *app_id       append a double-quoted identifier
  *app_str      append a single-quoted string literal
  *app_int      append an integer literal
  *app_float    append a floating-point literal
  *app_blob     append a hexadecimal blob literal
*/

typedef struct str_t {
    context_t *context;
    char *text;
    size_t size;
    size_t cap;
} str_t;

static void str_init(
    str_t *str,
    context_t *context)
{
    str->context=context;
    str->text=NULL;
    str->size=0;
    str->cap=0;
}

static void str_free(
    str_t *str)
{
    if (str->text) {
        sqlite3_free(str->text);
        str->text=NULL;
    }
    str->size=0;
    str->cap=0;
}

static int str_cap(
    str_t *str,
    size_t cap)
{
    if (cap>str->cap) {
        char *text;

        cap=cap+(cap>>2);
        text=crealloc(str->context,str->text,cap);
        if (!text)
            return -1;
        str->text=text;
        str->cap=cap;
    }
    return 0;
}

static int str8app(
    str_t *str,
    void const *text,
    size_t size)
{
    size_t newsize;

    newsize=str->size+size;
    if (str_cap(str,newsize))
        return -1;
    memcpy(str->text+str->size,text,size);
    str->size=newsize;
    return 0;
}

#define str8app_7 str8app

static int str8app_id(
    str_t *str,
    void const *text,
    size_t size)
{
    size_t newsize;
    unsigned char const *src;
    unsigned char const *end=(unsigned char const *)text+size;
    unsigned char *dst;

    newsize=str->size+size+2;
    src=text;
    while (src<end) {
        if (*src++=='"')
            newsize++;
    }
    if (str_cap(str,newsize))
        return -1;
    dst=(unsigned char *)(str->text+str->size);
    *dst++='"';
    src=text;
    while (src<end) {
        unsigned int c;

        c=*src++;
        *dst++=c;
        if (c=='"')
            *dst++=c;
    }
    *dst++='"';
    str->size=newsize;
    return 0;
}

static int str8app_str(
    str_t *str,
    void const *text,
    size_t size)
{
    size_t newsize;
    unsigned char const *src;
    unsigned char const *end=(unsigned char const *)text+size;
    unsigned char *dst;

    newsize=str->size+size+2;
    src=text;
    while (src<end) {
        if (*src++=='\'')
            newsize++;
    }
    if (str_cap(str,newsize))
        return -1;
    dst=(unsigned char *)(str->text+str->size);
    *dst++='\'';
    src=text;
    while (src<end) {
        unsigned int c;

        c=*src++;
        *dst++=c;
        if (c=='\'')
            *dst++=c;
    }
    *dst++='\'';
    str->size=newsize;
    return 0;
}

static int str8app_int(
    str_t *str,
    sqlite3_int64 i)
{
    char buf[24];

    sqlite3_snprintf(sizeof buf,buf,"%lld",i);
    return str8app_7(str,buf,strlen(buf));
}

static int str8app_float(
    str_t *str,
    double f)
{
    char buf[32];

    sqlite3_snprintf(sizeof buf,buf,"%.16g",f);
    return str8app_7(str,buf,strlen(buf));
}

static unsigned char const hex[16]="0123456789ABCDEF";

static int str8app_blob(
    str_t *str,
    void const *data,
    size_t size)
{
    size_t newsize;
    unsigned char const *src=data;
    unsigned char const *end=src+size;
    unsigned char *dst;

    newsize=str->size+3+size*4;
    if (str_cap(str,newsize))
        return -1;
    dst=(unsigned char *)(str->text+str->size);
    *dst++='x';
    *dst++='\'';
    while (src<end) {
        unsigned int c;

        c=*src++;
        *dst++=hex[c>>4 & 0xF];
        *dst++=hex[c    & 0xF];
    }
    *dst++='\'';
    str->size=newsize;
    return 0;
}

static int str16app_7(
    str_t *str,
    void const *text,
    size_t size)
{
    size_t newsize;
    unsigned char const *src;
    unsigned short *dst;

    newsize=str->size+size*2;
    if (str_cap(str,newsize))
        return -1;
    src=text;
    dst=(unsigned short *)(str->text+str->size);
    while (size-->0)
        *dst++=*src++;
    str->size=newsize;
    return 0;
}

static int str16app_id(
    str_t *str,
    void const *text,
    size_t size)
{
    size_t newsize;
    unsigned short const *src;
    unsigned short const *end=
        (unsigned short const *)((char const *)text+size);
    unsigned short *dst;

    newsize=str->size+size+4;
    src=text;
    while (src<end) {
        if (*src++=='"')
            newsize+=2;
    }
    if (str_cap(str,newsize))
        return -1;
    dst=(unsigned short *)(str->text+str->size);
    *dst++='"';
    src=text;
    while (src<end) {
        unsigned int c;

        c=*src++;
        *dst++=c;
        if (c=='"')
            *dst++=c;
    }
    *dst++='"';
    str->size=newsize;
    return 0;
}

static int str16app_str(
    str_t *str,
    void const *text,
    size_t size)
{
    size_t newsize;
    unsigned short const *src;
    unsigned short const *end=
        (unsigned short const *)((char const *)text+size);
    unsigned short *dst;

    newsize=str->size+size+4;
    src=text;
    while (src<end) {
        if (*src++=='\'')
            newsize+=2;
    }
    if (str_cap(str,newsize))
        return -1;
    dst=(unsigned short *)(str->text+str->size);
    *dst++='\'';
    src=text;
    while (src<end) {
        unsigned int c;

        c=*src++;
        *dst++=c;
        if (c=='\'')
            *dst++=c;
    }
    *dst++='\'';
    str->size=newsize;
    return 0;
}

static int str16app_int(
    str_t *str,
    sqlite3_int64 i)
{
    char buf[24];

    sqlite3_snprintf(sizeof buf,buf,"%lld",i);
    return str16app_7(str,buf,strlen(buf));
}

static int str16app_float(
    str_t *str,
    double f)
{
    char buf[32];

    sqlite3_snprintf(sizeof buf,buf,"%.16g",f);
    return str16app_7(str,buf,strlen(buf));
}

static int str16app_blob(
    str_t *str,
    void const *data,
    size_t size)
{
    size_t newsize;
    unsigned char const *src=data;
    unsigned char const *end=src+size;
    unsigned short *dst;

    newsize=str->size+3+size*4;
    if (str_cap(str,newsize))
        return -1;
    dst=(unsigned short *)(str->text+str->size);
    *dst++='x';
    *dst++='\'';
    while (src<end) {
        unsigned int c;

        c=*src++;
        *dst++=hex[c>>4 & 0xF];
        *dst++=hex[c    & 0xF];
    }
    *dst++='\'';
    str->size=newsize;
    return 0;
}

