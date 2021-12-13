/*
  (Fragments of) SQL statements with multiple uses.
*/

static char const pragmas_create_sql[] =
    "create table temp.pragmas( "
    "  phase integer not null, "
    "  name text not null, "
    "  value "
    ")";

static char const pragmas_insert_sql[] =
    "insert into temp.pragmas "
    "  values(?1,?2,?3)";

static char const pragmas_drop_sql[] =
    "drop table temp.pragmas";

static char const table_info_sql_1[] =
    "pragma table_info=";

static char const schema_drop_sql[] =
    "drop table temp.schema";

static char const rollback_sql[] =
    "rollback transaction";

