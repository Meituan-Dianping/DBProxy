#
# Parameters:
#
# $block_mode


--echo #### $block_mode



--echo #### 128-$block_mode

eval SET SESSION block_encryption_mode="aes-128-$block_mode";

--echo # must throw an error without an IV
--error ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT

--echo block mode dependent. Must be non-0 and non-null

--echo block mode dependent

--echo # must be equal

--echo # must not be equal


--echo #### 192-$block_mode

eval SET SESSION block_encryption_mode="aes-192-$block_mode";

--echo # must throw an error without an IV
--error ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT

--echo # must be equal

--echo # must not be equal


--echo #### 256-$block_mode

eval SET SESSION block_encryption_mode="aes-256-$block_mode";

--echo # must throw an error without an IV
--error ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT

--echo # must be equal

--echo # must not be equal

SET SESSION block_encryption_mode=DEFAULT;


--echo #### 128, 192 and 256 bit $block_mode


eval CREATE TABLE aes_$block_mode(a VARCHAR(128), b128 VARCHAR(144),
  b192 VARCHAR(144), b256 CHAR(144));
eval INSERT INTO aes_$block_mode (a) VALUES (REPEAT('a', 128));
eval INSERT INTO aes_$block_mode (a) VALUES (REPEAT(0x00313233, 32));

eval SET SESSION block_encryption_mode="aes-128-$block_mode";

eval SET SESSION block_encryption_mode="aes-192-$block_mode";

eval SET SESSION block_encryption_mode="aes-256-$block_mode";

--echo # must return 0
eval SELECT COUNT(*) FROM aes_$block_mode WHERE b128 = b192 OR B192 = b256 OR b128=b256;

eval SET SESSION block_encryption_mode="aes-256-$block_mode";

--echo # must return 2

--echo # must return 0

--echo # must return 0

eval SET SESSION block_encryption_mode="aes-192-$block_mode";

--echo # must return 2

--echo # must return 0

--echo # must return 0

eval SET SESSION block_encryption_mode="aes-128-$block_mode";

--echo # must return 2

--echo # must return 0

--echo # must return 0


SET SESSION block_encryption_mode=DEFAULT;
eval DROP TABLE aes_$block_mode;

