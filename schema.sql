PRAGMA foreign_keys=ON;
BEGIN TRANSACTION;
DROP TABLE IF EXISTS users;
CREATE TABLE users (
	username VARCHAR primary key,
	email VARCHAR,
	password VARCHAR(255)
);

DROP TABLE IF EXISTS followers;
CREATE TABLE followers (
	username VARCHAR,
	followerUsername VARCHAR,
	CONSTRAINT fk_users
	FOREIGN KEY (username)
	REFERENCES users(username)
	
);

DROP TABLE IF EXISTS posts;
CREATE TABLE posts (
	author VARCHAR,
	postContent VARCHAR,
	postTimestamp timestamp,
	CONSTRAINT fk_users
	FOREIGN KEY (author)
	REFERENCES users(username)
);
COMMIT;
