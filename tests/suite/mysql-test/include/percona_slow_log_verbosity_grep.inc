# Common extensions to the slow query log
--let grep_pattern = ^# Schema: .+  Last_errno: \d+  Killed: \d+\$
--let grep_pattern = ^#.*Rows_affected: \d+\$
--let grep_pattern = ^# Bytes_sent: \d+.*\$
# InnoDB
--let grep_pattern = ^# InnoDB_trx_id: \w+\$
# Query plan
--let grep_pattern = ^# Bytes_sent: \d+  Tmp_tables: \d+  Tmp_disk_tables: \d+  Tmp_table_sizes: \d+\$
--let grep_pattern = ^# QC_Hit: (Yes|No)  Full_scan: (Yes|No)  Full_join: (Yes|No)  Tmp_table: (Yes|No)  Tmp_table_on_disk: (Yes|No)\$
--let grep_pattern = ^# Filesort: (Yes|No)  Filesort_on_disk: (Yes|No)  Merge_passes: \d+\$
# InnoDB
--let grep_pattern = ^#   InnoDB_IO_r_ops: \d+  InnoDB_IO_r_bytes: \d+  InnoDB_IO_r_wait: \d*\.\d*\$
--let grep_pattern = ^#   InnoDB_rec_lock_wait: \d*\.\d*  InnoDB_queue_wait: \d*\.\d*\$
--let grep_pattern = ^#   InnoDB_pages_distinct: \d+\$
--let grep_pattern = ^# No InnoDB statistics available for this query\$
