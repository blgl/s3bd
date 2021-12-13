#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "s3bd.h"

static void usage(void)
{
    fputs(
        "Usage: s3bdload [ options ] dbfile [ pragma_override ... ]\n"
        "  options:\n"
        "    -i infile   # default is stdin\n"
        "    -s          # schema only\n"
        "  overrides:\n"
        "    name=value  # replace\n"
        "    name        # delete\n",
        stderr);
    exit(1);
}

int main(
    int argc,
    char **argv)
{
    int status;
    sqlite3 *connection;
    char *errmsg=NULL;
    char *inpath=NULL;
    unsigned int flags=0;
    char const * const *overrides;
    FILE *infile;

    for (;;) {
        int c;

        c=getopt(argc,argv,"si:");
        if (c==-1)
            break;
        switch (c) {
        case 'i':
            inpath=optarg;
            break;
        case 's':
            flags|=S3BD_LOAD_SCHEMA_ONLY;
            break;
        default:
            usage();
        }
    }
    argc-=optind;
    argv+=optind;
    if (argc<1)
        usage();
    if (inpath) {
        infile=fopen(inpath,"r");
        if (!infile) {
            fprintf(stderr,"%s: fopen: %s\n",
                    inpath,strerror(errno));
            return 1;
        }
    } else {
        infile=stdin;
    }
    if (argc>1) {
        overrides=(char const * const *)argv+1;
    } else {
        overrides=NULL;
    }

    status=sqlite3_open_v2(
        argv[0],
        &connection,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
        NULL);
    if (status!=SQLITE_OK) {
        if (connection) {
            fprintf(stderr,"%s: sqlite3_open: %s\n",
                    argv[0],sqlite3_errmsg(connection));
        } else {
            fprintf(stderr,"%s: sqlite3_open: %s\n",
                    argv[0],sqlite3_errstr(status));
        }
        return 1;
    }
    status=s3bd_load(connection,infile,flags,overrides,&errmsg);
    sqlite3_close(connection);
    if (status!=SQLITE_OK) {
        if (errmsg) {
            fprintf(stderr,"s3bd_load: %s\n",errmsg);
            sqlite3_free(errmsg);
        } else {
            fprintf(stderr,"s3bd_load: %s\n",sqlite3_errstr(status));
        }
        return 1;
    }
    return 0;
}

