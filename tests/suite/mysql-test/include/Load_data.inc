--echo ===============
--echo Load the data
--echo ===============


while ($i) {

eval INSERT INTO $table
dec $i;
}
commit;

