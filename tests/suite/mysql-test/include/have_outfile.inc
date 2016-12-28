-- require r/have_outfile.require
disable_query_log;
--remove_file $MYSQLTEST_VARDIR/tmp/outfile.test
enable_query_log;
