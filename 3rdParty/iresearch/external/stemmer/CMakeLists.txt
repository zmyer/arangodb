project(stemmer)

set(snowball_sources
  libstemmer_c/src_c/stem_ISO_8859_1_danish.c
  libstemmer_c/src_c/stem_ISO_8859_1_dutch.c
  libstemmer_c/src_c/stem_ISO_8859_1_english.c
  libstemmer_c/src_c/stem_ISO_8859_1_finnish.c
  libstemmer_c/src_c/stem_ISO_8859_1_french.c
  libstemmer_c/src_c/stem_ISO_8859_1_german.c
  libstemmer_c/src_c/stem_ISO_8859_1_italian.c
  libstemmer_c/src_c/stem_ISO_8859_1_norwegian.c
  libstemmer_c/src_c/stem_ISO_8859_1_porter.c
  libstemmer_c/src_c/stem_ISO_8859_1_portuguese.c
  libstemmer_c/src_c/stem_ISO_8859_1_spanish.c
  libstemmer_c/src_c/stem_ISO_8859_1_swedish.c
  libstemmer_c/src_c/stem_ISO_8859_2_hungarian.c
  libstemmer_c/src_c/stem_ISO_8859_2_romanian.c
  libstemmer_c/src_c/stem_KOI8_R_russian.c
  libstemmer_c/src_c/stem_UTF_8_danish.c
  libstemmer_c/src_c/stem_UTF_8_dutch.c
  libstemmer_c/src_c/stem_UTF_8_english.c
  libstemmer_c/src_c/stem_UTF_8_finnish.c
  libstemmer_c/src_c/stem_UTF_8_french.c
  libstemmer_c/src_c/stem_UTF_8_german.c
  libstemmer_c/src_c/stem_UTF_8_hungarian.c
  libstemmer_c/src_c/stem_UTF_8_italian.c
  libstemmer_c/src_c/stem_UTF_8_norwegian.c
  libstemmer_c/src_c/stem_UTF_8_porter.c
  libstemmer_c/src_c/stem_UTF_8_portuguese.c
  libstemmer_c/src_c/stem_UTF_8_romanian.c
  libstemmer_c/src_c/stem_UTF_8_russian.c
  libstemmer_c/src_c/stem_UTF_8_spanish.c
  libstemmer_c/src_c/stem_UTF_8_swedish.c
  libstemmer_c/src_c/stem_UTF_8_turkish.c
  libstemmer_c/runtime/api.c
  libstemmer_c/runtime/utilities.c
  libstemmer_c/libstemmer/libstemmer.c
)

add_library(stemmer STATIC ${snowball_sources})
target_include_directories(stemmer PUBLIC "libstemmer_c/include")
target_include_directories(stemmer PUBLIC
    "libstemmer_c/libstemmer"
    "libstemmer_c/src_c"
    "libstemmer_c/runtime"
)
