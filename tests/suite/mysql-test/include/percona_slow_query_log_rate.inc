
--source include/count_sessions.inc


--let i=2
--let k=1

while($i)
{
inc $k;
SELECT 'connection_one';

inc $k;
SELECT 'connection_two';

inc $k;
SELECT 'connection_three';

dec $i;
}

--let log_slow_rate_test=1
