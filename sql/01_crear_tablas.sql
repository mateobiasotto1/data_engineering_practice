-- ===============================================
-- BULK
-- ===============================================

-- Clientes
CREATE TABLE IF NOT EXISTS public.bulk_dim_cliente (
    customer_id   VARCHAR,
    customer_name VARCHAR,
    segment       VARCHAR,
    country       VARCHAR,
    city          VARCHAR,
    state         VARCHAR,
    postal_code   VARCHAR,
    region        VARCHAR
);

-- Productos
CREATE TABLE IF NOT EXISTS public.bulk_dim_producto (
    product_id    VARCHAR,
    product_name  VARCHAR,
    category      VARCHAR,
    sub_category  VARCHAR
);

-- Ventas
CREATE TABLE IF NOT EXISTS public.bulk_fact_ventas (
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

-- ===============================================
-- PUBLIC
-- ===============================================

-- Clientes
CREATE TABLE IF NOT EXISTS public.dim_cliente (
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
CREATE TABLE IF NOT EXISTS public.dim_producto (
    product_id    VARCHAR PRIMARY KEY,
    product_name  VARCHAR,
    category      VARCHAR,
    sub_category  VARCHAR
);

-- Ventas
CREATE TABLE IF NOT EXISTS public.fact_ventas (
    taligent_id  VARCHAR PRIMARY KEY,
    order_id     VARCHAR,
    order_date   DATE,
    ship_date    DATE,
    customer_id  VARCHAR REFERENCES public.dim_cliente(customer_id),
    product_id   VARCHAR REFERENCES public.dim_producto(product_id),
    sales        NUMERIC,
    quantity     INT,
    discount     NUMERIC,
    profit       NUMERIC
);

-- ===============================================
-- STAGING
-- ===============================================

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
