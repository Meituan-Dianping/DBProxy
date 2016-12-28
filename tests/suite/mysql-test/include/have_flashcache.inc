# check if binary compiled with flashcache support

-- require r/have_flashcache.require

disable_query_log;
show variables like 'have_flashcache';
enable_query_log;
