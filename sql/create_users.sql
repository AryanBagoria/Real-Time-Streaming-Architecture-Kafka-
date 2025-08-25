
DROP TABLE IF EXISTS users;

-- Create new table with raw JSON only - for staging purpose
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
