typedef struct conststr_t {
    void const *text;
    size_t size;
} conststr_t;

#define CONSTSTR(s) {(s),sizeof (s)}
#define CONSTSTR0(s) {(s),sizeof (s)-1}

/* are a and b equal? */

static int conststr_eq(
    conststr_t a,
    conststr_t b)
{
    return a.size==b.size && !memcmp(a.text,b.text,a.size);
}

/* is a a prefix of b? */

static int conststr_pref(
    conststr_t a,
    conststr_t b)
{
    return a.size<=b.size && !memcmp(a.text,b.text,a.size);
}

static conststr_t const encoding_names[4] =
{
    {NULL,0},
    CONSTSTR0("UTF-8"),
    CONSTSTR0("UTF-16le"),
    CONSTSTR0("UTF-16be")
};

