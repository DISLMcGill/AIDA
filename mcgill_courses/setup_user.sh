#!/bin/bash

$MONETDB/bin/monetdbd start /home/monet/dbfarm/

export DOTMONETDBFILE=/home/scripts/login_info.txt
$MONETDB/bin/mclient -d courses01 <<-END
START TRANSACTION;
CREATE ROLE copy_role;
GRANT COPY FROM TO copy_role;
CREATE USER "courses01" WITH PASSWORD 'courses01' NAME 'courses01' SCHEMA "sys";
CREATE SCHEMA "courses01" AUTHORIZATION "courses01";
ALTER USER "courses01" SET SCHEMA "courses01";
GRANT copy_role TO "courses01";
COMMIT;
END

$MONETDB/bin/mclient -d courses02 <<-END
START TRANSACTION;
CREATE ROLE copy_role;
GRANT COPY FROM TO copy_role;
CREATE USER "courses02" WITH PASSWORD 'courses02' NAME 'courses02' SCHEMA "sys";
CREATE SCHEMA "courses02" AUTHORIZATION "courses02";
ALTER USER "courses02" SET SCHEMA "courses02";
GRANT copy_role TO "courses02";
COMMIT;
END

$MONETDB/bin/mclient -d courses03 <<-END
START TRANSACTION;
CREATE ROLE copy_role;
GRANT COPY FROM TO copy_role;
CREATE USER "courses03" WITH PASSWORD 'courses03' NAME 'courses03' SCHEMA "sys";
CREATE SCHEMA "courses03" AUTHORIZATION "courses03";
ALTER USER "courses03" SET SCHEMA "courses03";
GRANT copy_role TO "courses03";
COMMIT;
END
