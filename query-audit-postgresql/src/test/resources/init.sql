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

CREATE TABLE index_semantics (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    email VARCHAR(255),
    note TEXT
);

CREATE UNIQUE INDEX idx_key_expression
    ON index_semantics (tenant_id, lower(email));
CREATE UNIQUE INDEX idx_key_include
    ON index_semantics (tenant_id, email) INCLUDE (note);
CREATE UNIQUE INDEX idx_key_partial
    ON index_semantics (tenant_id) WHERE email IS NOT NULL;
ALTER TABLE index_semantics
    ADD CONSTRAINT uq_deferred_email UNIQUE (email) DEFERRABLE INITIALLY DEFERRED;
