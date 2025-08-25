CREATE KEYSPACE IF NOT EXISTS analytics 
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE analytics;

CREATE TABLE IF NOT EXISTS users_clean (
    first_name text,
    last_name text,
    email text,
    city text,
    PRIMARY KEY (email)
);
