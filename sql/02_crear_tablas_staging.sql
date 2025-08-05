-- ===============================================
-- STAGING
-- ===============================================

-- Crear schema staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Clientes
CREATE TABLE IF NOT EXISTS staging.dim_cliente (
    customer_id   VARCHAR PRIMARY KEY,
    customer_name VARCHAR,
    segment       VARCHAR,
    country       VARCHAR,
    city          VARCHAR,
    state         VARCHAR,
    postal_code   VARCHAR,
    region        VARCHAR
);

-- Productos
CREATE TABLE IF NOT EXISTS staging.dim_producto (
    product_id    VARCHAR PRIMARY KEY,
    product_name  VARCHAR,
    category      VARCHAR,
    sub_category  VARCHAR
);

-- Ventas
CREATE TABLE IF NOT EXISTS staging.fact_ventas (
    taligent_id  VARCHAR PRIMARY KEY,
    order_id     VARCHAR,
    order_date   DATE,
    ship_date    DATE,
    customer_id  VARCHAR,
    product_id   VARCHAR,
    sales        NUMERIC,
    quantity     INT,
    discount     NUMERIC,
    profit       NUMERIC
);
