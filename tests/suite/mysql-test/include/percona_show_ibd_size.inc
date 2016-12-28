perl;
 my $table = $ENV{'IBD_TO_CHECK'};
 my $tablespace = $ENV{'MYSQLD_DATADIR'}."test/".$table.".ibd";
 my $size = -s "$tablespace";
 print "The size of $table tablespace file in bytes: $size\n";
 exit(0)
EOF
