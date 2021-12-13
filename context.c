/*
  A place to stash the top-level return value and error message,
  as well as some bits of state that are common to both storing and loading.
*/

typedef struct context_t {
    sqlite3 *connection;
    char *errmsg;
    char *nomem;
    int status;
    unsigned char db_enc;
    unsigned char native_enc;
    unsigned char double_end;
    unsigned char in_transaction;
} context_t;

static int context_init(
    context_t *context,
    sqlite3 *connection)
{
    context->connection=connection;
    context->errmsg=NULL;
    context->nomem=sqlite3_mprintf("%s",sqlite3_errstr(SQLITE_NOMEM));
    if (!context->nomem) {
        context->status=SQLITE_NOMEM;
        return -1;
    }
    context->status=SQLITE_OK;
    return 0;
}

static void errf(
    context_t *context,
    int status,
    char const *format,
    ...)
{
    va_list args;

    context->status=status;
    va_start(args,format);
    context->errmsg=sqlite3_vmprintf(format,args);
    va_end(args);
}

static void *cmalloc(
    context_t *context,
    size_t size)
{
    void *data;

    data=sqlite3_malloc(size);
    if (size>0 && !data)
        context->status=SQLITE_NOMEM;
    return data;
}

static void *crealloc(
    context_t *context,
    void *data,
    size_t size)
{
    data=sqlite3_realloc(data,size);
    if (size>0 && !data)
        context->status=SQLITE_NOMEM;
    return data;
}

static char const pragma_override_sql[] =
    "update temp.pragmas "
    "  set value=?2 "
    "  where name=?1";

static char const pragma_delete_sql[] =
    "delete from temp.pragmas "
    "  where name=?1";

static int override_pragmas(
    context_t *context,
    char const * const *overrides)
{
    sqlite3_stmt *update=NULL;
    sqlite3_stmt *delete=NULL;
    int status;
    char const *override;

    status=sqlite3_prepare_v2(
        context->connection,
        pragma_override_sql,sizeof pragma_override_sql,
        &update,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            context,status,
            "While overriding pragmas: sqlite3_prepare: %s",
            sqlite3_errmsg(context->connection));
        goto cleanup;
    }
    status=sqlite3_prepare_v2(
        context->connection,
        pragma_delete_sql,sizeof pragma_delete_sql,
        &delete,
        NULL);
    if (status!=SQLITE_OK) {
        errf(
            context,status,
            "While overriding pragmas: sqlite3_prepare: %s",
            sqlite3_errmsg(context->connection));
        goto cleanup;
    }
    while ((override=*overrides++)) {
        size_t size;
        char const *equals;

        size=strlen(override);
        equals=strchr(override,'=');
        if (equals) {
            status=sqlite3_bind_text(
                update,1,override,equals-override,SQLITE_STATIC);
            if (status!=SQLITE_OK) {
                errf(
                    context,status,
                    "While overriding pragmas: sqlite3_bind: %s",
                    sqlite3_errmsg(context->connection));
                goto cleanup;
            }
            equals++;
            status=sqlite3_bind_text(
                update,2,equals,override+size-equals,SQLITE_STATIC);
            if (status!=SQLITE_OK) {
                errf(
                    context,status,
                    "While overriding pragmas: sqlite3_bind: %s",
                    sqlite3_errmsg(context->connection));
                goto cleanup;
            }
            status=sqlite3_step(update);
            if (status!=SQLITE_DONE) {
                errf(
                    context,status,
                    "While overriding pragmas: sqlite3_step: %s",
                    sqlite3_errmsg(context->connection));
                goto cleanup;
            }
            sqlite3_reset(update);
            sqlite3_clear_bindings(update);
        } else {
            status=sqlite3_bind_text(delete,1,override,size,SQLITE_STATIC);
            if (status!=SQLITE_OK) {
                errf(
                    context,status,
                    "While overriding pragmas: sqlite3_bind: %s",
                    sqlite3_errmsg(context->connection));
                goto cleanup;
            }
            status=sqlite3_step(delete);
            if (status!=SQLITE_DONE) {
                errf(
                    context,status,
                    "While overriding pragmas: sqlite3_step: %s",
                    sqlite3_errmsg(context->connection));
                goto cleanup;
            }
            sqlite3_reset(delete);
            sqlite3_clear_bindings(delete);
        }
    }
    sqlite3_finalize(update);
    update=NULL;
    sqlite3_finalize(delete);
    delete=NULL;
    return 0;

cleanup:
    if (update)
        sqlite3_finalize(update);
    if (delete)
        sqlite3_finalize(delete);
    return -1;
}

static void rollback_transaction(
    context_t *context)
{
    if (context->in_transaction) {
        sqlite3_exec(context->connection,rollback_sql,0,NULL,NULL);
        context->in_transaction=0;
    }
}

static int context_term(
    context_t *context,
    char **errmsg)
{
    if (context->errmsg) {
        sqlite3_free(context->nomem);
    } else {
        context->errmsg=context->nomem;
    }
    context->nomem=NULL;
    if (errmsg) {
        *errmsg=context->errmsg;
    } else {
        sqlite3_free(context->errmsg);
    }
    context->errmsg=NULL;
    context->connection=NULL;
    return context->status;
}

static int prepare8(
    context_t *context,
    void const *text,
    size_t size,
    sqlite3_stmt **stmt)
{
    return sqlite3_prepare_v2(context->connection,text,size,stmt,NULL);
}

static int prepare16(
    context_t *context,
    void const *text,
    size_t size,
    sqlite3_stmt **stmt)
{
    return sqlite3_prepare16_v2(context->connection,text,size,stmt,NULL);
}

static int column_text8(
    context_t *context,
    sqlite3_stmt *stmt,
    int colix,
    conststr_t *text)
{
    text->text=sqlite3_column_text(stmt,colix);
    if (!text->text) {
        text->size=0;
        context->status=SQLITE_NOMEM;
        return -1;
    }
    text->size=sqlite3_column_bytes(stmt,colix);
    return 0;
}

static int column_text16(
    context_t *context,
    sqlite3_stmt *stmt,
    int colix,
    conststr_t *text)
{
    text->text=sqlite3_column_text16(stmt,colix);
    if (!text->text) {
        text->size=0;
        context->status=SQLITE_NOMEM;
        return -1;
    }
    text->size=sqlite3_column_bytes16(stmt,colix);
    return 0;
}

