DROP TABLE student CASCADE;
DROP TABLE course CASCADE;
DROP TABLE courseoffer CASCADE;
DROP TABLE enroll;

CREATE TABLE student
(
   sid INTEGER NOT NULL
  ,sname VARCHAR(30) NOT NULL
  ,PRIMARY KEY (sid)
);

CREATE TABLE course
(
   ccode VARCHAR(10) NOT NULL
  ,credits INTEGER NOT NULL
  ,dept VARCHAR(25) NOT NULL
  ,PRIMARY KEY(ccode)
);

CREATE TABLE courseoffer
(
   term VARCHAR(15) NOT NULL
  ,section INTEGER NOT NULL
  ,ccode VARCHAR(10) NOT NULL
  ,PRIMARY KEY(term, section, ccode)
  ,FOREIGN KEY(ccode) REFERENCES course(ccode)
);

CREATE TABLE enroll
(
   sid INTEGER NOT NULL
  ,term VARCHAR(15) NOT NULL
  ,section INTEGER NOT NULL
  ,ccode VARCHAR(10) NOT NULL
  ,grade VARCHAR(4)
  ,PRIMARY KEY(sid, term, section, ccode)
  ,FOREIGN KEY(term, section, ccode) REFERENCES courseoffer(term, section, ccode)
  ,FOREIGN KEY(sid) REFERENCES student(sid)
);

INSERT INTO student VALUES(12345678, 'Billy Joe');
INSERT INTO student VALUES(12345679, 'Sam Short');
INSERT INTO student VALUES(12345677, 'Moe Long');
INSERT INTO student VALUES(12345676, 'Jane Austin');
INSERT INTO student VALUES(12345675, 'Sally Lin');
INSERT INTO student VALUES(12345674, 'Martha Stewart');
INSERT INTO student VALUES(12345673, 'Lekha Mistri');
INSERT INTO student VALUES(12345672, 'Omar Sheikh');
INSERT INTO student VALUES(12345671, 'Tasmin Nur');
INSERT INTO student VALUES(12345670, 'Leila Li');

INSERT INTO course VALUES('comp-206', 3, 'computer science');
INSERT INTO course VALUES('comp-250', 3, 'computer science');
INSERT INTO course VALUES('comp-421', 3, 'computer science');
INSERT INTO course VALUES('comp-551', 4, 'computer science');
INSERT INTO course VALUES('comp-512', 4, 'computer science');
INSERT INTO course VALUES('comp-202', 3, 'computer science');
INSERT INTO course VALUES('comp-252', 3, 'computer science');
INSERT INTO course VALUES('comp-310', 3, 'computer science');
INSERT INTO course VALUES('comp-101', 1, 'computer science');
INSERT INTO course VALUES('comp-535', 1, 'computer science');
INSERT INTO course VALUES('comp-610', 1, 'computer science');
INSERT INTO course VALUES('ecse-429', 3, 'electrical engineering');
INSERT INTO course VALUES('ecse-321', 3, 'electrical engineering');
INSERT INTO course VALUES('math-240', 3, 'mathematics');
INSERT INTO course VALUES('math-241', 3, 'dept');

INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-101');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-206');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-250');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-252');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-310');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-421');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-421');
INSERT INTO courseoffer VALUES('winter 2016', 1, 'comp-421');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-551');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'comp-202');
INSERT INTO courseoffer VALUES('winter 2018', 2, 'comp-202');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-202');
INSERT INTO courseoffer VALUES('winter 2017', 2, 'comp-202');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-101');
INSERT INTO courseoffer VALUES('winter 2017', 2, 'comp-101');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-252');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'ecse-429');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'ecse-429');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'ecse-321');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'ecse-321');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-535');
INSERT INTO courseoffer VALUES('winter 2017', 1, 'comp-610');
INSERT INTO courseoffer VALUES('winter 2018', 1, 'math-240');

INSERT INTO enroll VALUES (12345679, 'winter 2018', 1, 'comp-421', NULL);
INSERT INTO enroll VALUES (12345679, 'winter 2018', 1, 'comp-551', NULL);
INSERT INTO enroll VALUES (12345679, 'winter 2018', 1, 'comp-202', 'A');
INSERT INTO enroll VALUES (12345679, 'winter 2018', 1, 'math-240', 'A');
INSERT INTO enroll VALUES (12345679, 'winter 2018', 1, 'ecse-321', 'A');
INSERT INTO enroll VALUES (12345679, 'winter 2017', 1, 'ecse-429', 'B+');

INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'comp-421', NULL);
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'comp-551', NULL);
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'comp-202', NULL);
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'comp-101', NULL);
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'comp-206', 'B');
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'comp-250', 'A');
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'math-240', 'A');
INSERT INTO enroll VALUES (12345678, 'winter 2018', 1, 'ecse-321', 'A');
INSERT INTO enroll VALUES (12345678, 'winter 2017', 1, 'comp-421', 'D');
INSERT INTO enroll VALUES (12345678, 'winter 2017', 1, 'ecse-321', 'B+');

INSERT INTO enroll VALUES (12345677, 'winter 2018', 1, 'comp-421', NULL);
INSERT INTO enroll VALUES (12345677, 'winter 2018', 1, 'ecse-429', NULL);
INSERT INTO enroll VALUES (12345677, 'winter 2018', 1, 'math-240', 'A');
INSERT INTO enroll VALUES (12345677, 'winter 2017', 1, 'comp-421', 'D');
INSERT INTO enroll VALUES (12345677, 'winter 2017', 1, 'ecse-429', 'D');
INSERT INTO enroll VALUES (12345677, 'winter 2018', 1, 'comp-551', NULL);
INSERT INTO enroll VALUES (12345677, 'winter 2018', 1, 'comp-202', 'A');

INSERT INTO enroll VALUES (12345676, 'winter 2018', 1, 'comp-421', 'B+');
INSERT INTO enroll VALUES (12345676, 'winter 2018', 1, 'comp-551', 'C');
INSERT INTO enroll VALUES (12345676, 'winter 2018', 2, 'comp-202', NULL);
INSERT INTO enroll VALUES (12345676, 'winter 2018', 1, 'ecse-321', 'A');
INSERT INTO enroll VALUES (12345676, 'winter 2017', 1, 'ecse-321', 'B+');
INSERT INTO enroll VALUES (12345676, 'winter 2018', 1, 'math-240', 'A');

INSERT INTO enroll VALUES (12345675, 'winter 2018', 2, 'comp-202', NULL);
INSERT INTO enroll VALUES (12345675, 'winter 2018', 1, 'comp-551', 'A');
INSERT INTO enroll VALUES (12345675, 'winter 2018', 1, 'comp-421', 'B');
INSERT INTO enroll VALUES (12345675, 'winter 2018', 1, 'math-240', 'A-');
INSERT INTO enroll VALUES (12345675, 'winter 2018', 1, 'ecse-429', 'A');

INSERT INTO enroll VALUES (12345674, 'winter 2017', 1, 'ecse-429', 'A-');

INSERT INTO enroll VALUES (12345673, 'winter 2017', 1, 'ecse-429', 'A');

INSERT INTO enroll VALUES (12345672, 'winter 2018', 1, 'comp-421', NULL);
INSERT INTO enroll VALUES (12345672, 'winter 2018', 1, 'comp-551', 'C+');
INSERT INTO enroll VALUES (12345672, 'winter 2018', 1, 'ecse-321', 'A');
INSERT INTO enroll VALUES (12345672, 'winter 2018', 1, 'math-240', 'A');

INSERT INTO enroll VALUES (12345671, 'winter 2018', 1, 'comp-421', NULL);
INSERT INTO enroll VALUES (12345671, 'winter 2018', 1, 'comp-551', 'A-');
INSERT INTO enroll VALUES (12345671, 'winter 2018', 1, 'ecse-321', 'A');
INSERT INTO enroll VALUES (12345671, 'winter 2018', 1, 'math-240', 'A');

INSERT INTO enroll VALUES (12345670, 'winter 2018', 1, 'comp-421', NULL);
INSERT INTO enroll VALUES (12345670, 'winter 2018', 1, 'comp-551', 'B+');
INSERT INTO enroll VALUES (12345670, 'winter 2018', 1, 'ecse-321', 'A');
INSERT INTO enroll VALUES (12345670, 'winter 2018', 1, 'math-240', 'A');
