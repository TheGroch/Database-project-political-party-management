CREATE EXTENSION pgcrypto;
CREATE TABLE member
(
    id int PRIMARY KEY,
    passwd varchar(128) NOT NULL,
    last_timestamp timestamp NOT NULL,
    is_leader boolean NOT NULL,
    upvotes int DEFAULT 0,
    downvotes int DEFAULT 0 
);

CREATE TABLE identifier
(
    id int PRIMARY KEY
);

CREATE TABLE projects
(
    id int PRIMARY KEY,
    authority_ID int NOT NULL
);

CREATE TABLE actions
(
    id int PRIMARY KEY,
    action_type varchar(7) NOT NULL,
    project_ID int NOT NULL REFERENCES projects(id),
    member_ID int NOT NULL REFERENCES member(id)
);

CREATE TABLE votes
(
    vote_type varchar(10) NOT NULL,
    member_ID int NOT NULL REFERENCES member(id),
    action_ID int NOT NULL REFERENCES actions(id) 
);
REVOKE ALL ON DATABASE student FROM PUBLIC;
CREATE ROLE app ENCRYPTED PASSWORD 'qwerty' LOGIN;
GRANT CONNECT ON DATABASE student TO app;
GRANT SELECT, UPDATE, INSERT ON ALL TABLES IN SCHEMA public TO app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO app;
