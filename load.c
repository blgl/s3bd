typedef struct load_context_t load_context_t;

/*
  Factored-out differences between the UTF-8 and UTF-16 modes of operation.
*/

typedef struct load_vt {
    int (*prepare)(
        context_t *context,
        void const *text,
        size_t size,
        sqlite3_stmt **stmt);
    int (*column_text)(
        context_t *context,
        sqlite3_stmt *stmt,
        int colix,
        conststr_t *text);
    int (*str_app_7)(
        str_t *str,
        void const *text,
        size_t size);
    int (*str_app_int)(
        str_t *str,
        sqlite3_int64 i);
    int (*str_app_float)(
        str_t *str,
        double f);
    int (*str_app_str)(
        str_t *str,
        void const *text,
        size_t size);
    int (*str_app_id)(
        str_t *str,
        void const *text,
        size_t size);
    int (*str_app_blob)(
        str_t *str,
        void const *data,
        size_t size);
    int (*read_text)(
        load_context_t *context,
        void *data,
        size_t size);
    conststr_t pragmas_id;
    conststr_t schema_id;
    conststr_t sqlite_sequence_id;
    conststr_t sqlite_stat_id;
} load_vt;

/*
  Extend the common context with load-specific parts.
*/

struct load_context_t {
    context_t c;
    FILE *infile;
    load_vt const *vt;
    sqlite3_stmt *store_pragma;
    sqlite3_stmt *count_pragmas;
    sqlite3_stmt *list_pragmas;
    sqlite3_stmt *store_object;
    sqlite3_stmt *list_objects;
    sqlite3_stmt *store_row;
    int defensive;
    unsigned char have_pragmas;
    unsigned char have_schema;
    unsigned char want_stat;
    unsigned char want_sequence;
    unsigned char want_virtuals;
};

static int rc(
    load_context_t *context)
{
    int c;

    c=getc(context->infile);
    if (c==EOF) {
        if (ferror(context->infile)) {
            errf(
                &context->c,SQLITE_IOERR_READ,
                "Read error: %s",strerror(errno));
        } else {
            errf(
                &context->c,SQLITE_IOERR_SHORT_READ,
                "Unexpected EOF");
        }
    }
    return c;
}

static int rd(
    load_context_t *context,
    void *data,
    size_t size)
{
    if (size>0 && !fread(data,size,1,context->infile)) {
        if (ferror(context->infile)) {
            errf(
                &context->c,SQLITE_IOERR_READ,
                "Read error: %s",strerror(errno));
        } else {
            errf(
                &context->c,SQLITE_IOERR_SHORT_READ,
                "Unexpected EOF");
        }
        return -1;
    }
    return 0;
}

static int read_text16(
    load_context_t *context,
    void *data,
    size_t size)
{
    if (context->c.db_enc==context->c.native_enc) {
        return rd(context,data,size);
    } else {
        FILE *infile=context->infile;
        unsigned char *dst=data;

        while (size>=2) {
            int c0,c1;

            if ((c0=getc(infile))==EOF
                    || (c1=getc(infile))==EOF) {
                if (ferror(infile)) {
                    errf(
                        &context->c,SQLITE_IOERR_READ,
                        "Read error: %s",strerror(errno));
                } else {
                    errf(
                        &context->c,SQLITE_IOERR_SHORT_READ,
                        "Unexpected EOF");
                }
                return -1;
            }
            *dst++=c1;
            *dst++=c0;
            size-=2;
        }
        return 0;
    }
}

static unsigned char const sqlite_sequence_id8[15] =
    "sqlite_sequence";

static unsigned short const sqlite_sequence_id16[15] =
{
    's','q','l','i','t','e','_','s','e','q','u','e','n','c','e'
};

static unsigned char const sqlite_stat_id8[11] =
    "sqlite_stat";

static unsigned short const sqlite_stat_id16[11] =
{
    's','q','l','i','t','e','_','s','t','a','t'
};


static load_vt const load_vt8 =
{
    prepare8,
    column_text8,
    str8app_7,
    str8app_int,
    str8app_float,
    str8app_str,
    str8app_id,
    str8app_blob,
    rd,
    CONSTSTR(s3bd_id8_pragmas),
    CONSTSTR(s3bd_id8_schema),
    CONSTSTR(sqlite_sequence_id8),
    CONSTSTR(sqlite_stat_id8)
};

static load_vt const load_vt16 =
{
    prepare16,
    column_text16,
    str16app_7,
    str16app_int,
    str16app_float,
    str16app_str,
    str16app_id,
    str16app_blob,
    read_text16,
    CONSTSTR(s3bd_id16_pragmas),
    CONSTSTR(s3bd_id16_schema),
    CONSTSTR(sqlite_sequence_id16),
    CONSTSTR(sqlite_stat_id16)
};

static int load_uint(
    load_context_t *context,
    unsigned int width,
    sqlite3_uint64 *result)
{
    unsigned char buf[8];
    sqlite3_uint64 u;
    unsigned int ix;

    if (width>8) {
        errf(
            &context->c,SQLITE_INTERNAL,
            "Internal error: integer width");
        return -1;
    }
    if (rd(context,buf,width))
        return -1;
    u=0;
    for (ix=0; ix<width; ix++) {
        u=u<<8 | buf[ix];
    }
    *result=u+s3bd_uint_bias[width];
    return 0;
}

static int load_sint(
    load_context_t *context,
    unsigned int width,
    sqlite3_int64 *result)
{
    unsigned char buf[8];
    sqlite3_uint64 u;
    unsigned int ix,flip;

    if (width>8) {
        errf(
            &context->c,SQLITE_INTERNAL,
            "Internal error: integer width");
        return -1;
    }
    if (rd(context,buf,width))
        return -1;
    if (width>0 && buf[0]&0x80) {
        flip=0xFF;
    } else {
        flip=0x00;
    }
    u=0;
    for (ix=0; ix<width; ix++) {
        u=u<<8 | (buf[ix]^flip);
    }
    u+=s3bd_sint_bias[width];
    if (flip) {
        *result=-(sqlite3_int64)u;
    } else {
        *result=u;
    }
    return 0;
}

static int load_float(
    load_context_t *context,
    unsigned int width,
    double *result)
{
    unsigned char buf[8];
    int ix,step,end;
    unsigned int bix;
    union {
        double f;
        unsigned char c[sizeof (double)];
    } convert;

    if (width>8) {
        errf(
            &context->c,SQLITE_INTERNAL,
            "Internal error: float width");
        return -1;
    }
    if (rd(context,buf,width))
        return -1;
    if (context->c.double_end==2) {
        ix=0;
        step=1;
        end=8;
    } else {
        ix=7;
        step=-1;
        end=-1;
    }
    for (bix=0; bix<width; bix++) {
        convert.c[ix]=buf[bix];
        ix+=step;
    }
    while (ix!=end) {
        convert.c[ix]=0;
        ix+=step;
    }
    *result=convert.f;
    return 0;
}

static int load_text(
    load_context_t *context,
    unsigned int width,
    conststr_t *result)
{
    sqlite3_uint64 size,u;
    void *data=NULL;

    if (width>8) {
        errf(
            &context->c,SQLITE_INTERNAL,
            "Internal error: text size width");
        goto cleanup;
    }
    if (load_uint(context,width,&u))
        goto cleanup;
    size=u;
    data=cmalloc(&context->c,size+1);
    if (!data)
        goto cleanup;
    if ((*context->vt->read_text)(context,data,size))
        goto cleanup;
    result->text=data;
    result->size=size;
    return 0;

cleanup:
    if (data)
        sqlite3_free(data);
    return -1;
}

/*
  SQLite offers no way to creats an sqlite3_value from scratch,
  so this will have to do instead.
*/

typedef struct intcol_t {
    int type;
    sqlite3_int64 val;
} intcol_t;

typedef struct floatcol_t {
    int type;
    double val;
} floatcol_t;

typedef struct textcol_t {
    int type;
    conststr_t text;
} textcol_t;

typedef struct blobcol_t {
    int type;
    void const *data;
    size_t size;
} blobcol_t;

typedef union col_t {
    int type;
    intcol_t intcol;
    floatcol_t floatcol;
    textcol_t textcol;
    blobcol_t blobcol;
} col_t;

static void col_free(
    col_t *col)
{
    switch (col->type) {
    case SQLITE_TEXT:
        sqlite3_free((void *)col->textcol.text.text);
        break;
    case SQLITE_BLOB:
        sqlite3_free((void *)col->blobcol.data);
        break;
    }
    col->type=SQLITE_NULL;
}

static int bind_col(
    load_context_t *context,
    sqlite3_stmt *stmt,
    int colix,
    col_t const *col)
{
    int status;

    switch (col->type) {
    case SQLITE_NULL:
        status=sqlite3_bind_null(stmt,colix);
        break;
    case SQLITE_INTEGER:
        status=sqlite3_bind_int64(stmt,colix,col->intcol.val);
        break;
    case SQLITE_FLOAT:
        status=sqlite3_bind_double(stmt,colix,col->floatcol.val);
        break;
    case SQLITE_TEXT:
        status=sqlite3_bind_text64(
            stmt,
            colix,
            col->textcol.text.text,col->textcol.text.size,
            SQLITE_STATIC,
            context->c.native_enc);
        break;
    case SQLITE_BLOB:
        status=sqlite3_bind_blob64(
            stmt,
            colix,
            col->blobcol.data,col->blobcol.size,
            SQLITE_STATIC);
        break;
    default:
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unknown column data type %d",col->type);
        return -1;
    }
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "sqlite_bind: %s",
            sqlite3_errmsg(context->c.connection));
        return -1;
    }
    return 0;
}

static int load_intcol(
    load_context_t *context,
    int marker,
    intcol_t *col)
{
    if (load_sint(context,INTCOL_iw(marker),&col->val))
        goto cleanup;
    col->type=SQLITE_INTEGER;
    return 0;

cleanup:
    col->type=SQLITE_NULL;
    return -1;
}

static int load_floatcol(
    load_context_t *context,
    int marker,
    floatcol_t *col)
{
    if (load_float(context,FLOATCOL_fw(marker),&col->val))
        goto cleanup;
    col->type=SQLITE_FLOAT;
    return 0;

cleanup:
    col->type=SQLITE_NULL;
    return -1;
}

static int load_textcol(
    load_context_t *context,
    int marker,
    textcol_t *col)
{
    if (load_text(context,TEXTCOL_tsw(marker),&col->text))
        goto cleanup;
    col->type=SQLITE_TEXT;
    return 0;

cleanup:
    col->type=SQLITE_NULL;
    return -1;
}

static int load_blobcol(
    load_context_t *context,
    int marker,
    blobcol_t *col)
{
    sqlite3_uint64 size,u;
    void *data=NULL;

    if (load_uint(context,BLOBCOL_bsw(marker),&u))
        goto cleanup;
    size=u;
    data=cmalloc(&context->c,size+1);
    if (!data)
        goto cleanup;
    if (rd(context,data,size))
        goto cleanup;
    col->data=data;
    col->size=size;
    col->type=SQLITE_BLOB;
    return 0;

cleanup:
    if (data)
        sqlite3_free(data);
    col->type=SQLITE_NULL;
    return -1;
}

typedef int (*row_cb)(
    load_context_t *context,
    size_t colcnt,
    col_t const *values);

typedef row_cb (*head_cb)(
    load_context_t *context,
    conststr_t setname,
    size_t colcnt);

static int load_rowset(
    load_context_t *context,
    int marker,
    head_cb dohead)
{
    col_t *cols=NULL;
    row_cb dorow;
    conststr_t name;
    sqlite3_uint64 u;
    size_t colcnt,colix;
    int c;

    name.text=NULL;
    if (load_uint(context,ROWSET_ccw(marker),&u))
        goto cleanup;
    colcnt=u+1;
    if (load_text(context,ROWSET_nsw(marker),&name))
        goto cleanup;
    cols=cmalloc(&context->c,colcnt*sizeof (col_t));
    if (!cols)
        goto cleanup;
    for (colix=0; colix<colcnt; colix++) {
        cols[colix].type=SQLITE_NULL;
    }
    dorow=(*dohead)(context,name,colcnt);
    if (!dorow)
        goto cleanup;
    for (;;) {
        for (colix=0; colix<colcnt; colix++) {
            c=rc(context);
            if (is_NULLCOL(c)) {
                cols[colix].type=SQLITE_NULL;
            } else if (is_INTCOL(c)) {
                if (load_intcol(context,c,&cols[colix].intcol))
                    goto cleanup;
            } else if (is_FLOATCOL(c)) {
                if (load_floatcol(context,c,&cols[colix].floatcol))
                    goto cleanup;
            } else if (is_TEXTCOL(c)) {
                if (load_textcol(context,c,&cols[colix].textcol))
                    goto cleanup;
            } else if (is_BLOBCOL(c)) {
                if (load_blobcol(context,c,&cols[colix].blobcol))
                    goto cleanup;
            } else if (c==EOF) {
                goto cleanup;
            } else {
                if (colix==0)
                    goto no_more_rows;
                errf(
                    &context->c,SQLITE_CORRUPT,
                    "Unexpected input");
                goto cleanup;
            }
        }
        if ((*dorow)(context,colcnt,cols))
            goto cleanup;
        for (colix=0; colix<colcnt; colix++) {
            col_free(&cols[colix]);
        }
    }
no_more_rows:
    if (!is_ENDSET(c)) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected input");
        goto cleanup;
    }
    sqlite3_free(cols);
    cols=NULL;
    sqlite3_free((void *)name.text);
    name.text=NULL;
    return 0;

cleanup:
    if (cols) {
        for (colix=0; colix<colcnt; colix++) {
            col_free(&cols[colix]);
        }
        sqlite3_free(cols);
    }
    if (name.text)
        sqlite3_free((void *)name.text);
    return -1;
}

static char const encoding_sql_1[] =
    "pragma encoding=";

static char const foreign_keys_sql[] =
    "pragma foreign_keys=0";

static int load_header(
    load_context_t *context)
{
    s3bd_header_t header;
    unsigned int encoding;
    str_t sql;
    char *errmsg=NULL;
    int status;

    str_init(&sql,&context->c);
    if (rd(context,&header,sizeof header))
        goto cleanup;
    if (memcmp(header.magic,s3bd_header_magic,sizeof header.magic)) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Not an SQLite3 binary dump file");
        goto cleanup;
    }
    if (header.ver_major!=CURVER_MAJOR || header.ver_minor!=CURVER_MINOR) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unsupported dump format version %u.%u",
            header.ver_major,header.ver_minor);
        goto cleanup;
    }
    encoding=header.encoding;
    switch (encoding) {
    case SQLITE_UTF8:
        context->c.native_enc=encoding;
        context->vt=&load_vt8;
        break;
    case SQLITE_UTF16LE:
    case SQLITE_UTF16BE:
        context->c.native_enc=endian_short();
        if (!context->c.native_enc) {
            errf(
                &context->c,SQLITE_ERROR,
                "Failed to determine short integer endianness");
            goto cleanup;
        }
        context->vt=&load_vt16;
        break;
    default:
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unsupported dump encoding value %u",encoding);
        goto cleanup;
    }
    context->c.db_enc=encoding;
    context->c.double_end=endian_double();
    if (!context->c.double_end) {
        errf(
            &context->c,SQLITE_ERROR,
            "Unsupported floating-point format");
        goto cleanup;
    }

    if (str8app_7(&sql,encoding_sql_1,sizeof encoding_sql_1-1))
        goto cleanup;
    if (str8app_str(
            &sql,encoding_names[encoding].text,encoding_names[encoding].size))
        goto cleanup;
    if (str8app_7(&sql,"",1))
        goto cleanup;
    status=sqlite3_exec(context->c.connection,sql.text,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to set database encoding: %s",
            errmsg);
        goto cleanup;
    }
    str_free(&sql);
    return 0;

cleanup:
    str_free(&sql);
    return -1;
}

static int pragma_row(
    load_context_t *context,
    size_t colcnt,
    col_t const *cols)
{
    sqlite3_stmt *store_pragma=context->store_pragma;
    size_t colix;
    int status;

    if (colcnt!=3
            || cols[0].type!=SQLITE_INTEGER
            || cols[1].type!=SQLITE_TEXT) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected pragmas rowset data");
        goto cleanup;
    }
    for (colix=0; colix<3; colix++) {
        if (bind_col(context,store_pragma,colix+1,&cols[colix]))
            goto cleanup;
    }
    status=sqlite3_step(store_pragma);
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While reading pragmas: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_reset(store_pragma);
    sqlite3_clear_bindings(store_pragma);
    return 0;

cleanup:
    sqlite3_reset(store_pragma);
    sqlite3_clear_bindings(store_pragma);
    return -1;
}

static row_cb pragmas_head(
    load_context_t *context,
    conststr_t setname,
    size_t colcnt)
{
    if (!conststr_eq(context->vt->pragmas_id,setname)) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected pragmas rowset name");
        return (row_cb)0;
    }
    if (colcnt!=3) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected pragmas rowset column count");
        return (row_cb)0;
    }
    return pragma_row;
}


static char const pragmas_count_sql[] =
    "select count(*) from temp.pragmas "
    "  where phase=?1";

static char const pragmas_list_sql[] =
    "select name,value from temp.pragmas "
    "  where phase=?1";

static int load_pragmas(
    load_context_t *context)
{
    char *errmsg=NULL;
    int status;
    int c;

    c=rc(context);
    if (c==EOF)
        goto cleanup;
    if (!is_ROWSET(c)) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected input");
        goto cleanup;
    }
    status=sqlite3_exec(
        context->c.connection,pragmas_create_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Unable to create pragmas temporary table: %s",
            errmsg);
        goto cleanup;
    }
    context->have_pragmas=1;
    status=sqlite3_prepare_v2(
        context->c.connection,
        pragmas_insert_sql,sizeof pragmas_insert_sql,
        &context->store_pragma,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading pragmas: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    if (load_rowset(context,c,pragmas_head))
        goto cleanup;
    sqlite3_finalize(context->store_pragma);
    context->store_pragma=NULL;

    status=sqlite3_prepare_v2(
        context->c.connection,
        pragmas_count_sql,sizeof pragmas_count_sql,
        &context->count_pragmas,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading pragmas: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    status=sqlite3_prepare_v2(
        context->c.connection,
        pragmas_list_sql,sizeof pragmas_list_sql,
        &context->list_pragmas,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading pragmas: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    if (context->store_pragma) {
        sqlite3_finalize(context->store_pragma);
        context->store_pragma=NULL;
    }
    return -1;
}

static int apply_pragmas(
    load_context_t *context,
    int phase)
{
    load_vt const *vt=context->vt;
    sqlite3_stmt *count=context->count_pragmas;
    sqlite3_stmt *list=context->list_pragmas;
    sqlite3_stmt *apply=NULL;
    str_t sql;
    sqlite3_int64 pragmacnt=-1;
    sqlite3_int64 pragmaix;
    size_t *offsets=NULL;
    int status;

    status=sqlite3_bind_int(count,1,phase);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While applying pragmas: sqlite3_bind: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    for (;;) {
        status=sqlite3_step(count);
        if (status!=SQLITE_ROW)
            break;
        pragmacnt=sqlite3_column_int64(count,0);
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While applying pragmas: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_reset(count);
    if (pragmacnt<0) {
        errf(
            &context->c,SQLITE_ERROR,
            "Failed to count phase %d pragmas",phase);
        goto cleanup;
    }
    if (pragmacnt<1)
        return 0;

    offsets=cmalloc(&context->c,(pragmacnt+1)*sizeof (size_t));
    if (!offsets)
        goto cleanup;
    str_init(&sql,&context->c);
    offsets[0]=0;
    status=sqlite3_bind_int(list,1,phase);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While applying pragmas: sqlite3_bind: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    pragmaix=0;
    for (;;) {
        conststr_t name;
        conststr_t value;
        void const *data;
        size_t size;

        status=sqlite3_step(list);
        if (status!=SQLITE_ROW)
            break;
        if (pragmaix>=pragmacnt) {
            errf(
                &context->c,SQLITE_INTERNAL,
                "Internal error: pragma count");
            goto cleanup;
        }
        if ((*vt->column_text)(&context->c,list,0,&name))
            goto cleanup;
        if ((*vt->str_app_7)(&sql,"pragma ",7))
            goto cleanup;
        if ((*vt->str_app_id)(&sql,name.text,name.size))
            goto cleanup;
        if ((*vt->str_app_7)(&sql,"=",1))
            goto cleanup;
        switch (sqlite3_column_type(list,1)) {
        case SQLITE_NULL:
            if ((*vt->str_app_7)(&sql,"null",4))
                goto cleanup;
            break;
        case SQLITE_INTEGER:
            if ((*vt->str_app_int)(&sql,sqlite3_column_int64(list,1)))
                goto cleanup;
            break;
        case SQLITE_FLOAT:
            if ((*vt->str_app_float)(&sql,sqlite3_column_double(list,1)))
                goto cleanup;
            break;
        case SQLITE_TEXT:
            if ((*vt->column_text)(&context->c,list,1,&value))
                goto cleanup;
            if ((*vt->str_app_str)(&sql,value.text,value.size))
                goto cleanup;
            break;
        case SQLITE_BLOB:
            data=sqlite3_column_blob(list,1);
            size=sqlite3_column_bytes(list,1);
            if (size>0 && !data) {
                context->c.status=SQLITE_NOMEM;
                goto cleanup;
            }
            if ((*vt->str_app_blob)(&sql,data,size))
                goto cleanup;
            break;
        default:
            errf(
                &context->c,SQLITE_CORRUPT,
                "Unknown pragma value type");
            goto cleanup;
        }
        if ((*vt->str_app_7)(&sql,"",1))
            goto cleanup;
        offsets[++pragmaix]=sql.size;
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While applying pragmas: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_reset(list);
    if (pragmaix<pragmacnt) {
        errf(
            &context->c,SQLITE_INTERNAL,
            "Internal error: pragma count");
        goto cleanup;
    }
    for (pragmaix=0; pragmaix<pragmacnt; pragmaix++) {
        status=(*vt->prepare)(
            &context->c,
            sql.text+offsets[pragmaix],
            offsets[pragmaix+1]-offsets[pragmaix],
            &apply);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While applying pragmas: sqlite3_prepare: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        do {
            status=sqlite3_step(apply);
        } while (status==SQLITE_ROW);
        if (status!=SQLITE_DONE) {
            errf(
                &context->c,status,
                "While applying pragmas: sqlite3_step: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        sqlite3_finalize(apply);
        apply=NULL;
    }
    sqlite3_free(offsets);
    offsets=NULL;
    str_free(&sql);
    return 0;

cleanup:
    str_free(&sql);
    if (apply)
        sqlite3_finalize(apply);
    if (offsets)
        sqlite3_free(offsets);
    return -1;
}

static void load_done_pragmas(
    load_context_t *context)
{
    if (context->list_pragmas) {
        sqlite3_finalize(context->list_pragmas);
        context->list_pragmas=NULL;
    }
    if (context->have_pragmas) {
        sqlite3_exec(context->c.connection,pragmas_drop_sql,0,NULL,NULL);
        context->have_pragmas=0;
    }
}

static char const schema_create_sql[] =
    "create table temp.schema( "
    "  phase integer not null, "
    "  name text not null, "
    "  sql text not null "
    ")";

static char const schema_insert_sql[] =
    "insert into temp.schema "
    "  values(?1,?2,?3)";

static int schema_row(
    load_context_t *context,
    size_t colcnt,
    col_t const *cols)
{
    load_vt const *vt=context->vt;
    sqlite3_stmt *store_object=context->store_object;
    int status;
    size_t colix;

    if (colcnt!=3
            || cols[0].type!=SQLITE_INTEGER
            || cols[1].type!=SQLITE_TEXT
            || cols[2].type!=SQLITE_TEXT) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected schema rowset data");
        goto cleanup;
    }
    switch (cols[0].intcol.val) {
    case SCHEMA_PHASE_TABLE:
        /* The sqlite_sequence table can't be created directly,
           so just remember that it's wanted. */
        if (conststr_eq(vt->sqlite_sequence_id,cols[1].textcol.text)) {
            context->want_sequence=1;
            return 0;
        }
        /* Ditto the various sqlite_stat* tables. */
        if (conststr_pref(vt->sqlite_stat_id,cols[1].textcol.text)) {
            context->want_stat=1;
            return 0;
        }
        break;
    case SCHEMA_PHASE_VIRTUAL_TABLE:
        context->want_virtuals=1;
        break;
    }
    for (colix=0; colix<3; colix++) {
        if (bind_col(context,store_object,colix+1,&cols[colix]))
            goto cleanup;
    }
    status=sqlite3_step(store_object);
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While reading schema: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_reset(store_object);
    sqlite3_clear_bindings(store_object);
    return 0;

cleanup:
    sqlite3_reset(store_object);
    sqlite3_clear_bindings(store_object);
    return -1;
}

static row_cb schema_head(
    load_context_t *context,
    conststr_t setname,
    size_t colcnt)
{
    if (!conststr_eq(context->vt->schema_id,setname)) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected schema rowset name");
        return (row_cb)0;
    }
    if (colcnt!=3) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected schema rowset column count");
        return (row_cb)0;
    }
    return schema_row;
}

static char const sequence_create_sql[] =
    "create table \"\"(\"\" integer primary key autoincrement);"
    "drop table \"\";";

static char const stat_create_sql[] =
    "analyze sqlite_schema";

static char const schema_list_sql[] =
    "select name,sql from temp.schema "
    "  where phase=?1";

static int load_schema(
    load_context_t *context)
{
    int status;
    char *errmsg=NULL;
    sqlite3_stmt *list_tables=NULL;
    int c;

    c=rc(context);
    if (c==EOF)
        goto cleanup;
    if (!is_ROWSET(c)) {
        errf(
            &context->c,SQLITE_CORRUPT,
            "Unexpected input");
        goto cleanup;
    }
    status=sqlite3_exec(context->c.connection,schema_create_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to create temporary schema table: %s",
            errmsg);
        goto cleanup;
    }
    context->have_schema=1;

    status=sqlite3_prepare_v2(
        context->c.connection,
        schema_insert_sql,sizeof schema_insert_sql,
        &context->store_object,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading schema: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    if (load_rowset(context,c,schema_head))
        goto cleanup;
    sqlite3_finalize(context->store_object);
    context->store_object=NULL;

    status=sqlite3_prepare_v2(
        context->c.connection,
        schema_list_sql,sizeof schema_list_sql,
        &context->list_objects,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading schema: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    return 0;

cleanup:
    if (list_tables)
        sqlite3_finalize(list_tables);
    if (errmsg)
        sqlite3_free(errmsg);
    if (context->store_object) {
        sqlite3_finalize(context->store_object);
        context->store_object=NULL;
    }
    return -1;
}

static int create_system_tables(
    load_context_t *context)
{
    char *errmsg=NULL;
    int status;

    if (context->want_sequence) {
        /* Indirectly create the sqlite_sequence table by creating and
           immediately dropping a table with an autoincrement column. */
        status=sqlite3_exec(
            context->c.connection,
            sequence_create_sql,
            0,
            NULL,
            &errmsg);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "Failed to create sqlite_sequence table: %s",
                errmsg);
            goto cleanup;
        }
    }
    if (context->want_stat) {
        /* Create whatever sqlite_stat* tables this SQLite was configured
           to use.  Hopefully, it's the same ones the source database had. */
        status=sqlite3_exec(
            context->c.connection,
            stat_create_sql,
            0,
            NULL,
            &errmsg);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "Failed to create sqlite_stat* table: %s",
                errmsg);
            goto cleanup;
        }
    }
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    return -1;
}

static int create_objects(
    load_context_t *context,
    int phase)
{
    load_vt const *vt=context->vt;
    sqlite3_stmt *list=context->list_objects;
    sqlite3_stmt *create=NULL;
    int status;

    status=sqlite3_bind_int(list,1,phase);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While creating objects: sqlite3_bind: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    for (;;) {
        conststr_t sql;

        status=sqlite3_step(list);
        if (status!=SQLITE_ROW)
            break;
        if ((*vt->column_text)(&context->c,list,1,&sql))
            goto cleanup;
        status=(*vt->prepare)(&context->c,sql.text,sql.size,&create);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While creating objects: sqlite3_prepare: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        status=sqlite3_step(create);
        if (status!=SQLITE_DONE) {
            errf(
                &context->c,status,
                "While creating objects: sqlite3_step: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        sqlite3_finalize(create);
        create=NULL;
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While creating objects: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_reset(list);
    return 0;

cleanup:
    sqlite3_reset(list);
    if (create)
        sqlite3_finalize(create);
    return -1;
}

static char const writable_schema_on_sql[] =
    "pragma writable_schema=1";

static char const schema_sneak_sql[] =
    "insert into sqlite_schema(type,name,tbl_name,rootpage,sql) "
    "  select ?2,name,name,0,sql from temp.schema "
    "    where phase=?1";

static char const writable_schema_off_sql[] =
    "pragma writable_schema=reset";

static int create_sneaky(
    load_context_t *context,
    int phase,
    conststr_t type)
{
    char *errmsg=NULL;
    sqlite3_stmt *sneak=NULL;
    int writable=0;
    int status;

    status=sqlite3_exec(
        context->c.connection,writable_schema_on_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Unable to set the schema writable: %s",
            errmsg);
        goto cleanup;
    }
    writable=1;
    status=sqlite3_prepare_v2(
        context->c.connection,
        schema_sneak_sql,sizeof schema_sneak_sql,
        &sneak,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While sneaking objects into the schema: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    status=sqlite3_bind_int(sneak,1,phase);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While sneaking objects into the schema: sqlite3_bind: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    status=sqlite3_bind_text(sneak,2,type.text,type.size,SQLITE_STATIC);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While sneaking objects into the schema: sqlite3_bind: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    status=sqlite3_step(sneak);
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While sneaking objects into the schema: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_finalize(sneak);
    sneak=NULL;
    sqlite3_exec(context->c.connection,writable_schema_off_sql,0,NULL,NULL);
    writable=0;
    return 0;

cleanup:
    if (sneak)
        sqlite3_finalize(sneak);
    if (errmsg)
        sqlite3_free(errmsg);
    if (writable)
        sqlite3_exec(context->c.connection,writable_schema_off_sql,0,NULL,NULL);
    return -1;
}

static void load_done_schema(
    load_context_t *context)
{
    if (context->list_objects) {
        sqlite3_finalize(context->list_objects);
        context->list_objects=NULL;
    }
    if (context->have_schema) {
        sqlite3_exec(context->c.connection,schema_drop_sql,0,NULL,NULL);
        context->have_schema=0;
    }
}

static int table_row(
    load_context_t *context,
    size_t colcnt,
    col_t const *cols)
{
    sqlite3_stmt *store_row=context->store_row;
    size_t colix;
    int status;

    for (colix=0; colix<colcnt; colix++) {
        if (bind_col(context,store_row,colix+1,&cols[colix]))
            goto cleanup;
    }
    status=sqlite3_step(store_row);
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While storing tables: sqlite_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_reset(store_row);
    sqlite3_clear_bindings(store_row);
    return 0;

cleanup:
    sqlite3_reset(store_row);
    sqlite3_clear_bindings(store_row);
    return -1;
}

static int ignore_row(
    load_context_t *context,
    size_t colcnt,
    col_t const *cols)
{
    (void)context;
    (void)colcnt;
    (void)cols;
    return 0;
}

static char const sequence_clear_sql[] =
    "delete from sqlite_sequence";

static char const store_data_sql_1[] =
    "insert into ";
static char const store_data_sql_2[] =
    " values (";
static char const store_data_sql_3[] =
    ")";

static row_cb table_head(
    load_context_t *context,
    conststr_t setname,
    size_t colcnt)
{
    load_vt const *vt=context->vt;
    str_t sql;
    sqlite3_stmt *table_info=NULL;
    char *errmsg=NULL;
    int status;
    size_t xcolcnt,colix;

    str_init(&sql,&context->c);
    if ((*vt->str_app_7)(&sql,table_info_sql_1,sizeof table_info_sql_1-1))
        goto cleanup;
    if ((*vt->str_app_id)(&sql,setname.text,setname.size))
        goto cleanup;
    status=(*vt->prepare)(&context->c,sql.text,sql.size,&table_info);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading tables: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sql.size=0;
    xcolcnt=0;
    for (;;) {
        status=sqlite3_step(table_info);
        if (status!=SQLITE_ROW)
            break;
        xcolcnt++;
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While reading tables: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_finalize(table_info);
    table_info=NULL;
    if (xcolcnt!=colcnt) {
        /* No matching table found in the new database.  This can happen
           even for correct input when the SQLite library used on the other
           end was configured to use different sqlite_stat* tables,
           so in that case, just ignore all rows rather than failing. */
        if (conststr_pref(vt->sqlite_stat_id,setname)) {
            str_free(&sql);
            return ignore_row;
        }
        errf(
            &context->c,SQLITE_CORRUPT,
            "Rowset not in schema");
        goto cleanup;
    }

    if (conststr_eq(vt->sqlite_sequence_id,setname)) {
        status=sqlite3_exec(
            context->c.connection,sequence_clear_sql,0,NULL,&errmsg);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "Failed to clear sqlite_sequence table: %s",
                errmsg);
            goto cleanup;
        }
    }

    if ((*vt->str_app_7)(&sql,store_data_sql_1,sizeof store_data_sql_1-1))
        goto cleanup;
    if ((*vt->str_app_id)(&sql,setname.text,setname.size))
        goto cleanup;
    if ((*vt->str_app_7)(&sql,store_data_sql_2,sizeof store_data_sql_2-1))
        goto cleanup;
    for (colix=0; colix<colcnt; colix++) {
        if (colix>0 && (*vt->str_app_7)(&sql,",",1))
            goto cleanup;
        if ((*vt->str_app_7)(&sql,"?",1))
            goto cleanup;
    }
    if ((*vt->str_app_7)(&sql,store_data_sql_3,sizeof store_data_sql_3-1))
        goto cleanup;
    status=(*vt->prepare)(&context->c,sql.text,sql.size,&context->store_row);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While reading tables: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    str_free(&sql);
    return table_row;

cleanup:
    str_free(&sql);
    if (table_info)
        sqlite3_finalize(table_info);
    return (row_cb)0;
}

static int load_tables(
    load_context_t *context)
{
    for (;;) {
        int c;

        c=rc(context);
        if (c==EOF)
            goto cleanup;
        if (is_ENDDUMP(c))
            break;
        if (!is_ROWSET(c)) {
            errf(
                &context->c,SQLITE_CORRUPT,
                "Unexpected input");
            goto cleanup;
        }
        if (load_rowset(context,c,table_head))
            goto cleanup;
        if (context->store_row) {
            sqlite3_finalize(context->store_row);
            context->store_row=NULL;
        }
    }
    return 0;

cleanup:
    if (context->store_row) {
        sqlite3_finalize(context->store_row);
        context->store_row=NULL;
    }
    return -1;
}

static char const getpages_sql[] =
    "pragma page_count";

static char const begin_immediate_sql[] =
    "begin immediate transaction";

static char const commit_sql[] =
    "commit transaction";

static int disable_defensive(
    load_context_t *context)
{
    int status;

    status=sqlite3_db_config(
        context->c.connection,SQLITE_DBCONFIG_DEFENSIVE,
        0,&context->defensive);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to turn off defensive mode: %s",
            sqlite3_errstr(status));
        return -1;
    }
    return 0;
}

static void restore_defensive(
    load_context_t *context)
{
    if (context->defensive) {
        sqlite3_db_config(
            context->c.connection,SQLITE_DBCONFIG_DEFENSIVE,
            context->defensive,(int *)0);
        context->defensive=0;
    }
}

static int check_pristine(
    load_context_t *context)
{
    sqlite3_stmt *getpages=NULL;
    sqlite3_int64 pagecount=-1;
    int status;

    status=sqlite3_prepare_v2(
        context->c.connection,
        getpages_sql,sizeof getpages_sql,
        &getpages,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While getting page count: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    for (;;) {
        status=sqlite3_step(getpages);
        if (status!=SQLITE_ROW)
            break;
        pagecount=sqlite3_column_int64(getpages,0);
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While getting page count: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_finalize(getpages);
    getpages=NULL;
    if (pagecount==-1) {
        errf(
            &context->c,SQLITE_ERROR,
            "Failed to get page count");
        goto cleanup;
    }
    if (pagecount>0) {
        errf(
            &context->c,SQLITE_CANTOPEN,
            "Database already exists");
        goto cleanup;
    }
    return 0;

cleanup:
    if (getpages)
        sqlite3_finalize(getpages);
    return -1;
}

static int disable_foreign_keys(
    load_context_t *context)
{
    char *errmsg=NULL;
    int status;

    status=sqlite3_exec(context->c.connection,foreign_keys_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to disable foreign key checks: %s",
            errmsg);
        goto cleanup;
    }
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    return -1;
}

static int load_begin_transaction(
    load_context_t *context)
{
    char *errmsg=NULL;
    int status;

    status=sqlite3_exec(
        context->c.connection,begin_immediate_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to start transaction: %s",errmsg);
        goto cleanup;
    }
    context->c.in_transaction=1;
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    return -1;
}

static int commit_transaction(
    load_context_t *context)
{
    char *errmsg=NULL;
    int status;

    status=sqlite3_exec(context->c.connection,commit_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to commit transaction: %s",errmsg);
        goto cleanup;
    }
    context->c.in_transaction=0;
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    return -1;
}

static conststr_t const table =
    CONSTSTR0("table");

int s3bd_load(
    sqlite3 *connection,
    FILE *infile,
    unsigned int flags,
    char const * const *overrides,
    char **errmsg)
{
    load_context_t context;

    memset(&context,0,sizeof context);
    context.infile=infile;
    context.store_pragma=NULL;
    context.list_pragmas=NULL;
    context.store_object=NULL;
    context.list_objects=NULL;
    context.store_row=NULL;
    if (context_init(&context.c,connection))
        goto cleanup;

    if (disable_defensive(&context))
        goto cleanup;
    if (check_pristine(&context))
        goto cleanup;
    if (disable_foreign_keys(&context))
        goto cleanup;
    if (load_header(&context))
        goto cleanup;
    if (load_pragmas(&context))
        goto cleanup;
    if (overrides) {
        if (override_pragmas(&context.c,overrides))
            goto cleanup;
    }
    if (apply_pragmas(&context,PRAGMA_PHASE_PRE_TRANSACTION))
        goto cleanup;
    if (load_begin_transaction(&context))
        goto cleanup;
    if (apply_pragmas(&context,PRAGMA_PHASE_IN_TRANSACTION))
        goto cleanup;
    if (load_schema(&context))
        goto cleanup;
    if (create_system_tables(&context))
        goto cleanup;
    if (create_objects(&context,SCHEMA_PHASE_TABLE))
        goto cleanup;
    if (!(flags & S3BD_LOAD_SCHEMA_ONLY)) {
        if (load_tables(&context))
            goto cleanup;
    }
    if (create_objects(&context,SCHEMA_PHASE_INDEX))
        goto cleanup;
    if (context.want_virtuals
            && create_sneaky(&context,SCHEMA_PHASE_VIRTUAL_TABLE,table))
        goto cleanup;
    if (create_objects(&context,SCHEMA_PHASE_VIEW))
        goto cleanup;
    if (create_objects(&context,SCHEMA_PHASE_TRIGGER))
        goto cleanup;
    load_done_schema(&context);
    if (commit_transaction(&context))
        goto cleanup;
    if (apply_pragmas(&context,PRAGMA_PHASE_POST_TRANSACTION))
        goto cleanup;
    load_done_pragmas(&context);
    restore_defensive(&context);
    context_term(&context.c,errmsg);
    return SQLITE_OK;

cleanup:
    load_done_schema(&context);
    load_done_pragmas(&context);
    rollback_transaction(&context.c);
    restore_defensive(&context);
    return context_term(&context.c,errmsg);
}

