LDLIBS=-lsqlite3
OPTCFLAGS=-Os
WARNCFLAGS=-Wall -Wextra
CFLAGS=$(OPTCFLAGS) $(WARNCFLAGS)

LIBOBJ=s3bd.o s3bdformat.o

all:	s3bdstore s3bdload libs3bd.a

s3bdstore:	s3bdstore.o s3bd.o s3bdformat.o

s3bdload:	s3bdload.o s3bd.o s3bdformat.o

libs3bd.a:	$(LIBOBJ)
	ar -r libs3bd.a $(LIBOBJ)
	ranlib libs3bd.a

clean:
	rm -f *.o *~ s3bdstore s3bdload libs3bd.a

s3bdstore.o: s3bdstore.c s3bd.h
s3bdload.o: s3bdload.c s3bd.h
s3bd.o: s3bd.c store.c load.c conststr.c sql.c context.c str.c endian.c \
	s3bd.h s3bdformat.h
s3bdformat.o: s3bdformat.c s3bdformat.h
