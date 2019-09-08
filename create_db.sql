CREATE SCHEMA prj1;

CREATE TABLE prj1.user (
	uid    INT NOT NULL PRIMARY KEY,
	source CHAR(12),
	region CHAR(10),
	cost   FLOAT
);

CREATE TABLE prj1.log (
	uid    INT NOT NULL,
	date   DATE,
	event_type CHAR(8),
	sum    FLOAT,
	CONSTRAINT log_user_uid_fk FOREIGN KEY(uid) REFERENCES prj1.user(uid)
);

\COPY prj1.user FROM '/tmp/DB/table_user.csv' DELIMITER ',' CSV HEADER;
\COPY prj1.log FROM '/tmp/DB/table_log.csv' DELIMITER ',' CSV HEADER;
