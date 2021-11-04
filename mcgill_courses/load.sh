#!/bin/bash

SF=$1
export DOTMONETDBFILE="/home/data/mcgill_courses/login_info$SF.txt"
$MONETDB/bin/mclient -d courses$SF setup.sql
$MONETDB/bin/mclient -d courses$SF <<-END
START TRANSACTION;
SET ROLE copy_role;
COPY INTO student from '$PWD/students$SF.csv' USING DELIMITERS ',', '\n';
COPY INTO course from '$PWD/courses$SF.csv' USING DELIMITERS ',', '\n';
COPY INTO courseoffer from '$PWD/course_offers$SF.csv' USING DELIMITERS ',', '\n';
COPY INTO enroll from '$PWD/enrolls$SF.csv' USING DELIMITERS ',', '\n';
COMMIT;
END
unset DOTMONETDB
