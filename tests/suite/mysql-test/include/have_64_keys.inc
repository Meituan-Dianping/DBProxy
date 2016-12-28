# Check that we have MAX_INDEXES=64

--require r/have_64_keys.require

# Check that maximum length of possible_keys and key_len fields is 4096
# They are defined in THD::send_explain_fields as NAME_CHAR_LEN*MAX_KEY
# where NAME_CHAR_LEN is always 64 (mysql_com.h) and MAX_KEY is
# a synonym of MAX_INDEXES

--enable_metadata
EXPLAIN SELECT 1;
--disable_metadata
