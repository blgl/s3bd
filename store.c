typedef struct store_context_t store_context_t;

/*
  Factored-out differences between the UTF-8 and UTF-16 modes of operation.
*/

typedef struct store_vt {
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
    int (*str_app_id)(
        str_t *str,
        void const *text,
        size_t size);
    int (*write_text)(
        store_context_t *context,
        void const *text,
        size_t size);
    conststr_t pragmas_id;
    conststr_t schema_id;
} store_vt;

/*
  Extend the common context with store-specific parts.
*/

struct store_context_t {
    context_t c;
    FILE *outfile;
    store_vt const *vt;
    unsigned char have_pragmas;
    unsigned char have_schema;
};

static int wd(
    store_context_t *context,
    void const *data,
    size_t size)
{
    if (size>0 && !fwrite(data,size,1,context->outfile)) {
        errf(
            &context->c,SQLITE_IOERR_WRITE,
            "Write error: %s",strerror(errno));
        return -1;
    }
    return 0;
}

static int wc(
    store_context_t *context,
    int c)
{
    if (putc(c,context->outfile)==EOF) {
        errf(
            &context->c,SQLITE_IOERR_WRITE,
            "Write error: %s",strerror(errno));
        return -1;
    }
    return 0;
}

static int write_text16(
    store_context_t *context,
    void const *text,
    size_t size)
{
    if (context->c.db_enc==context->c.native_enc) {
        return wd(context,text,size);
    } else {
        FILE *outfile=context->outfile;
        unsigned char const *src=text;

        while (size>=2) {
            unsigned int c0,c1;

            c0=src[0];
            c1=src[1];
            if (putc(c1,outfile)==EOF || putc(c0,outfile)==EOF) {
                errf(
                    &context->c,SQLITE_IOERR_WRITE,
                    "Write error: %s",strerror(errno));
                return -1;
            }
            src+=2;
            size-=2;
        }
    }
    return 0;
}

static store_vt store_vt8 =
{
    prepare8,
    column_text8,
    str8app_7,
    str8app_id,
    wd,
    CONSTSTR(s3bd_id8_pragmas),
    CONSTSTR(s3bd_id8_schema)
};

static store_vt store_vt16 =
{
    prepare16,
    column_text16,
    str16app_7,
    str16app_id,
    write_text16,
    CONSTSTR(s3bd_id16_pragmas),
    CONSTSTR(s3bd_id16_schema)
};

static unsigned int encode_uint(
    unsigned char *buf,
    sqlite3_uint64 u)
{
    unsigned int width,ix;

    for (width=0; width<8 && u>=s3bd_uint_bias[width+1]; width++)
        ;
    u-=s3bd_uint_bias[width];
    for (ix=1; ix<=width; ix++) {
        buf[width-ix]=u;
        u>>=8;
    }
    return width;
}

static unsigned int encode_sint(
    unsigned char *buf,
    sqlite3_int64 i)
{
    sqlite3_uint64 u;
    unsigned int flip;
    unsigned int width,ix;

    if (i<0) {
        u=-i;
        flip=0xFF;
    } else {
        u=i;
        flip=0x00;
    }
    for (width=0; width<8 && u>=s3bd_sint_bias[width+1]; width++)
        ;
    u-=s3bd_sint_bias[width];
    for (ix=1; ix<=width; ix++) {
        buf[width-ix]=u^flip;
        u>>=8;
    }
    return width;
}

static int store_intcol(
    store_context_t *context,
    sqlite3_int64 i)
{
    unsigned char buf[9];
    unsigned int width;

    width=encode_sint(buf+1,i);
    buf[0]=INTCOL(width);
    return wd(context,buf,1+width);
}

static unsigned int encode_float(
    unsigned char *buf,
    double f,
    int endian)
{
    union {
        double f;
        unsigned char c[sizeof (double)];
    } convert;
    int ix,step,end;
    unsigned int width;

    convert.f=f;
    if (endian==2) {
        ix=0;
        step=1;
        end=8;
    } else {
        ix=7;
        step=-1;
        end=-1;
    }
    while (end!=ix && !convert.c[end-step])
        end-=step;
    width=0;
    while (ix!=end) {
        buf[width]=convert.c[ix];
        ix+=step;
        width++;
    }
    return width;
}

static int store_floatcol(
    store_context_t *context,
    double f)
{
    unsigned char buf[9];
    unsigned int width;

    width=encode_float(buf+1,f,context->c.double_end);
    buf[0]=FLOATCOL(width);
    return wd(context,buf,1+width);
}

static int store_textcol(
    store_context_t *context,
    void const *text,
    size_t size)
{
    unsigned char buf[9];
    unsigned int width;

    width=encode_uint(buf+1,size);
    buf[0]=TEXTCOL(width);
    if (wd(context,buf,1+width))
        return -1;
    if ((*context->vt->write_text)(context,text,size))
        return -1;
    return 0;
}

static int store_blobcol(
    store_context_t *context,
    void const *data,
    size_t size)
{
    unsigned char buf[9];
    unsigned int width;

    width=encode_uint(buf+1,size);
    buf[0]=BLOBCOL(width);
    if (wd(context,buf,1+width))
        return -1;
    if (wd(context,data,size))
        return -1;
    return 0;
}

static int store_rowset(
    store_context_t *context,
    conststr_t ident,
    sqlite3_stmt *stmt)
{
    store_vt const *vt=context->vt;
    unsigned char buf[17];
    unsigned int namewidth,colswidth;
    int status;
    int colcnt,colix;

    colcnt=sqlite3_column_count(stmt);
    if (!colcnt)
        return 0;
    colswidth=encode_uint(buf+1,colcnt-1);
    namewidth=encode_uint(buf+1+colswidth,ident.size);
    buf[0]=ROWSET(colswidth,namewidth);
    if (wd(context,buf,1+colswidth+namewidth))
        return -1;
    if ((*vt->write_text)(context,ident.text,ident.size))
        return -1;
    for (;;) {
        status=sqlite3_step(stmt);
        if (status!=SQLITE_ROW)
            break;
        if (colcnt!=sqlite3_data_count(stmt)) {
            errf(
                &context->c,SQLITE_ERROR,
                "While extracting rows: Column count mismatch");
            return -1;
        }
        for (colix=0; colix<colcnt; colix++) {
            int type;
            conststr_t text;
            void const *blob;
            size_t size;

            type=sqlite3_column_type(stmt,colix);
            switch (type) {
            case SQLITE_NULL:
                if (wc(context,NULLCOL()))
                    return -1;
                break;
            case SQLITE_INTEGER:
                if (store_intcol(context,sqlite3_column_int64(stmt,colix)))
                    return -1;
                break;
            case SQLITE_FLOAT:
                if (store_floatcol(context,sqlite3_column_double(stmt,colix)))
                    return -1;
                break;
            case SQLITE_TEXT:
                if ((*vt->column_text)(&context->c,stmt,colix,&text))
                    return -1;
                if (store_textcol(context,text.text,text.size))
                    return -1;
                break;
            case SQLITE_BLOB:
                size=sqlite3_column_bytes(stmt,colix);
                if (size>0) {
                    blob=sqlite3_column_blob(stmt,colix);
                    if (!blob) {
                        context->c.status=SQLITE_NOMEM;
                        return -1;
                    }
                } else {
                    blob=NULL;
                }
                if (store_blobcol(context,blob,size))
                    return -1;
                break;
            default:
                errf(
                    &context->c,SQLITE_ERROR,
                    "While extracting rows: Unknown column type %d",type);
                return -1;
            }
        }
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While extracting rows: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        return -1;
    }
    return wc(context,ENDSET());
}

static char const getenc_sql[] =
    "pragma encoding";

static int store_header(
    store_context_t *context)
{
    int status;
    sqlite3_stmt *get=NULL;
    int encoding=-1;
    s3bd_header_t header;

    status=sqlite3_prepare_v2(
        context->c.connection,
        getenc_sql,sizeof getenc_sql,
        &get,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While getting database encoding: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    for (;;) {
        conststr_t encname;
        int encix;

        status=sqlite3_step(get);
        if (status!=SQLITE_ROW)
            break;
        if (column_text8(&context->c,get,0,&encname))
            goto cleanup;
        for (encix=1; encix<=3; encix++) {
            if (conststr_eq(encname,encoding_names[encix])) {
                encoding=encix;
                break;
            }
        }
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While getting database encoding: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    sqlite3_finalize(get);
    get=NULL;
    if (encoding<1) {
        errf(
            &context->c,SQLITE_ERROR,
            "Failed to determine database text encoding");
        goto cleanup;
    }

    memcpy(header.magic,s3bd_header_magic,sizeof header.magic);
    header.ver_major=CURVER_MAJOR;
    header.ver_minor=CURVER_MINOR;
    header.encoding=encoding;
    if (wd(context,&header,sizeof header))
        goto cleanup;
    context->c.db_enc=encoding;
    if (encoding>SQLITE_UTF8) {
        context->vt=&store_vt16;
        context->c.native_enc=endian_short();
        if (!context->c.native_enc) {
            errf(
                &context->c,SQLITE_ERROR,
                "Failed to determine short integer endianness");
            goto cleanup;
        }
    } else {
        context->vt=&store_vt8;
        context->c.native_enc=encoding;
    }
    context->c.double_end=endian_double();
    if (!context->c.double_end) {
        errf(
            &context->c,SQLITE_ERROR,
            "Unsupported floating-point format");
        goto cleanup;
    }
    return 0;

cleanup:
    if (get)
        sqlite3_finalize(get);
    return -1;
}

typedef struct pragma_def_t {
    int phase;
    conststr_t name;
} pragma_def_t;

static pragma_def_t const pragma_defs[] =
{
    {PRAGMA_PHASE_PRE_TRANSACTION,	CONSTSTR0("page_size")},
    {PRAGMA_PHASE_PRE_TRANSACTION,	CONSTSTR0("auto_vacuum")},

    {PRAGMA_PHASE_IN_TRANSACTION,	CONSTSTR0("application_id")},
    {PRAGMA_PHASE_IN_TRANSACTION,	CONSTSTR0("user_version")},

    {PRAGMA_PHASE_POST_TRANSACTION,	CONSTSTR0("journal_mode")},

    {0, {NULL, 0}}
};

static char const pragmas_select_sql[] =
    "select * from temp.pragmas";

static char const pragma_sql_1[] =
    "pragma ";

static int extract_pragmas(
    store_context_t *context)
{
    int status;
    char *errmsg=NULL;
    sqlite3_stmt *remember=NULL;
    sqlite3_stmt *extract=NULL;
    str_t sql;
    pragma_def_t const *def;

    str_init(&sql,&context->c);
    status=sqlite3_exec(
        context->c.connection,pragmas_create_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to create temporary pragmas table: %s",errmsg);
        goto cleanup;
    }
    context->have_pragmas=1;

    status=sqlite3_prepare_v2(
        context->c.connection,
        pragmas_insert_sql,sizeof pragmas_insert_sql,
        &remember,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While getting pragma values: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    for (def=pragma_defs; def->name.text; def++) {
        status=sqlite3_bind_int(remember,1,def->phase);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While getting pragma %s value: sqlite3_bind: %s",
                def->name.text,sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        status=sqlite3_bind_text(
            remember,
            2,
            def->name.text,def->name.size,
            SQLITE_STATIC);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While getting pragma %s value: sqlite3_bind: %s",
                def->name.size,sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        if (str8app_7(&sql,pragma_sql_1,sizeof pragma_sql_1-1))
            goto cleanup;
        if (str8app_id(&sql,def->name.text,def->name.size))
            goto cleanup;
        status=sqlite3_prepare_v2(
            context->c.connection,
            sql.text,sql.size,
            &extract,
            NULL);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While getting pragma %s value: sqlite3_prepare: %s",
                def->name.text,sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        str_free(&sql);
        for (;;) {
            sqlite3_value *value;

            status=sqlite3_step(extract);
            if (status!=SQLITE_ROW)
                break;
            value=sqlite3_column_value(extract,0);
            if (!value) {
                context->c.status=SQLITE_NOMEM;
                goto cleanup;
            }
            status=sqlite3_bind_value(remember,3,value);
            if (status!=SQLITE_OK) {
                errf(
                    &context->c,status,
                    "While getting pragma %s value: sqlite3_bind: %s",
                    def->name.text,sqlite3_errmsg(context->c.connection));
                goto cleanup;
            }
            status=sqlite3_step(remember);
            if (status!=SQLITE_DONE) {
                errf(
                    &context->c,status,
                    "While getting pragma %s value: sqlite3_step: %s",
                    def->name.text,sqlite3_errmsg(context->c.connection));
                goto cleanup;
            }
            sqlite3_reset(remember);
            sqlite3_bind_null(remember,2);
        }
        if (status!=SQLITE_DONE) {
            errf(
                &context->c,status,
                "While getting pragma %s value: sqlite3_step: %s",
                def->name.text,sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        sqlite3_finalize(extract);
        extract=NULL;
    }
    sqlite3_finalize(remember);
    remember=NULL;
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    str_free(&sql);
    if (remember)
        sqlite3_finalize(remember);
    if (extract)
        sqlite3_finalize(extract);
    return -1;
}

static int store_pragmas(
    store_context_t *context)
{
    int status;
    sqlite3_stmt *get_rows;

    status=sqlite3_prepare_v2(
        context->c.connection,
        pragmas_select_sql,sizeof pragmas_select_sql,
        &get_rows,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While getting pragma values: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    if (store_rowset(context,context->vt->pragmas_id,get_rows))
        goto cleanup;
    sqlite3_finalize(get_rows);
    get_rows=NULL;
    return 0;

cleanup:
    if (get_rows)
        sqlite3_finalize(get_rows);
    return -1;
}

static void store_done_pragmas(
    store_context_t *context)
{
    if (context->have_pragmas) {
        sqlite3_exec(context->c.connection,pragmas_drop_sql,0,NULL,NULL);
        context->have_pragmas=0;
    }
}

#define __(x) #x
#define _(x) __(x)

static char const schema_extract_sql[] =
    "create table temp.schema as "
    "select "
    "  case type "
    "  when 'table' then "
    "    case "
    "    when rootpage>0 then "
    "      " _(SCHEMA_PHASE_TABLE) " "
    "    else "
    "      " _(SCHEMA_PHASE_VIRTUAL_TABLE) " "
    "    end "
    "  when 'index' then "
    "    " _(SCHEMA_PHASE_INDEX) " "
    "  when 'view' then "
    "    " _(SCHEMA_PHASE_VIEW) " "
    "  when 'trigger' then "
    "    " _(SCHEMA_PHASE_TRIGGER) " "
    "  end as phase, "
    "  name, "
    "  sql "
    "from sqlite_schema "
    "  where sql is not null";

static char const schema_select_sql[] =
    "select * from temp.schema";

static char const table_names_sql[] =
    "select name from temp.schema "
    "  where phase=" _(SCHEMA_PHASE_TABLE) " "
    "  order by name='sqlite_sequence'";

static char const get_rows_sql_1[] =
    "select ";
static char const get_rows_sql_2[] =
    " from ";

static int store_schema(
    store_context_t *context)
{
    store_vt const *vt=context->vt;
    char *errmsg=NULL;
    sqlite3_stmt *get_rows=NULL;
    int status;

    status=sqlite3_exec(
        context->c.connection,schema_extract_sql,0,NULL,&errmsg);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "Failed to extract schema: %s",errmsg);
        goto cleanup;
    }
    context->have_schema=1;
    status=sqlite3_prepare_v2(
        context->c.connection,
        schema_select_sql,sizeof schema_select_sql,
        &get_rows,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While extracting schema: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    if (store_rowset(context,vt->schema_id,get_rows))
        goto cleanup;
    sqlite3_finalize(get_rows);
    get_rows=NULL;
    return 0;

cleanup:
    if (errmsg)
        sqlite3_free(errmsg);
    if (get_rows)
        sqlite3_finalize(get_rows);
    return -1;
}

static void store_done_schema(
    store_context_t *context)
{
    if (context->have_schema) {
        sqlite3_exec(context->c.connection,schema_drop_sql,0,NULL,NULL);
        context->have_schema=0;
    }
}

static int store_tables(
    store_context_t *context)
{
    store_vt const *vt=context->vt;
    sqlite3_stmt *list_tables=NULL;
    sqlite3_stmt *list_columns=NULL;
    sqlite3_stmt *get_rows=NULL;
    str_t sql;
    int status;

    str_init(&sql,&context->c);
    status=sqlite3_prepare_v2(
        context->c.connection,
        table_names_sql, sizeof table_names_sql,
        &list_tables,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            &context->c,status,
            "While extracting schema: sqlite3_prepare: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    for (;;) {
        conststr_t tablename;
        int colcnt;

        status=sqlite3_step(list_tables);
        if (status!=SQLITE_ROW)
            break;
        if ((*vt->column_text)(&context->c,list_tables,0,&tablename))
            goto cleanup;
        if ((*vt->str_app_7)(&sql,table_info_sql_1,sizeof table_info_sql_1-1))
            goto cleanup;
        if ((*vt->str_app_id)(&sql,tablename.text,tablename.size))
            goto cleanup;
        status=(*vt->prepare)(&context->c,sql.text,sql.size,&list_columns);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While extracting tables: sqlite3_prepare: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        sql.size=0;
        if ((*vt->str_app_7)(&sql,get_rows_sql_1,sizeof get_rows_sql_1-1))
            goto cleanup;
        colcnt=0;
        for (;;) {
            conststr_t colname;

            status=sqlite3_step(list_columns);
            if (status!=SQLITE_ROW)
                break;
            if ((*vt->column_text)(&context->c,list_columns,1,&colname))
                goto cleanup;
            if (colcnt>0) {
                if ((*vt->str_app_7)(&sql,",",1))
                    goto cleanup;
            }
            if ((*vt->str_app_id)(&sql,colname.text,colname.size))
                goto cleanup;
            colcnt++;
        }
        if (status!=SQLITE_DONE) {
            errf(
                &context->c,status,
                "While extracting tables: sqlite3_step: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        if (!colcnt) {
            errf(
                &context->c,SQLITE_ERROR,
                "While extracting tables: pragma table_info returned no rows");
            goto cleanup;
        }
        sqlite3_finalize(list_columns);
        list_columns=NULL;
        if ((*vt->str_app_7)(&sql,get_rows_sql_2,sizeof get_rows_sql_2-1))
            goto cleanup;
        if ((*vt->str_app_id)(&sql,tablename.text,tablename.size))
            goto cleanup;
        status=(*vt->prepare)(&context->c,sql.text,sql.size,&get_rows);
        if (status!=SQLITE_OK) {
            errf(
                &context->c,status,
                "While extracting tables: sqlite3_prepare: %s",
                sqlite3_errmsg(context->c.connection));
            goto cleanup;
        }
        sql.size=0;

        if (store_rowset(context,tablename,get_rows))
            goto cleanup;
        sqlite3_finalize(get_rows);
        get_rows=NULL;
    }
    if (status!=SQLITE_DONE) {
        errf(
            &context->c,status,
            "While extracting tables: sqlite3_step: %s",
            sqlite3_errmsg(context->c.connection));
        goto cleanup;
    }
    str_free(&sql);
    sqlite3_finalize(list_tables);
    list_tables=NULL;
    return 0;

cleanup:
    if (list_tables)
        sqlite3_finalize(list_tables);
    if (list_columns)
        sqlite3_finalize(list_columns);
    if (get_rows)
        sqlite3_finalize(get_rows);
    str_free(&sql);
    return -1;
}

static char const begin_sql[] =
    "begin transaction";

static int store_begin_transaction(
    store_context_t *context)
{
    char *errmsg=NULL;
    int status;

    sqlite3_busy_timeout(context->c.connection,999999999);
    status=sqlite3_exec(context->c.connection,begin_sql,0,NULL,&errmsg);
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

static int store_end(
    store_context_t *context)
{
    if (wc(context,ENDDUMP()))
        return -1;
    if (fflush(context->outfile)) {
        errf(
            &context->c,SQLITE_IOERR_WRITE,
            "Write error: %s",strerror(errno));
        return -1;
    }
    return 0;
}

int s3bd_store(
    sqlite3 *connection,
    FILE *outfile,
    unsigned int flags,
    char const * const *overrides,
    char **errmsg)
{
    store_context_t context;

    memset(&context,0,sizeof context);
    context.outfile=outfile;
    if (context_init(&context.c,connection))
        goto cleanup;

    if (!(flags & S3BD_STORE_IN_TRANSACTION)
            && store_begin_transaction(&context))
        goto cleanup;
    if (store_header(&context))
        goto cleanup;
    if (extract_pragmas(&context))
        goto cleanup;
    if (overrides) {
        if (override_pragmas(&context.c,overrides))
            goto cleanup;
    }
    if (store_pragmas(&context))
        goto cleanup;
    store_done_pragmas(&context);
    if (store_schema(&context))
        goto cleanup;
    if (!(flags & S3BD_STORE_SCHEMA_ONLY)) {
        if (store_tables(&context))
            goto cleanup;
    }
    store_done_schema(&context);
    rollback_transaction(&context.c);
    if (store_end(&context))
        goto cleanup;
    context_term(&context.c,errmsg);
    return SQLITE_OK;

cleanup:
    store_done_pragmas(&context);
    store_done_schema(&context);
    rollback_transaction(&context.c);
    return context_term(&context.c,errmsg);
}

