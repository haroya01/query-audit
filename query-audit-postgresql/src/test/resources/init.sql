CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    username VARCHAR(100) NOT NULL,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_email ON users (email);
CREATE INDEX idx_username ON users (username);
CREATE INDEX idx_status_created ON users (status, created_at);

CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE INDEX idx_user_id ON orders (user_id);
CREATE INDEX idx_status ON orders (status);
