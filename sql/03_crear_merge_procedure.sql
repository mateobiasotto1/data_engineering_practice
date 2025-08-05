-- ===============================================
-- STORED PROCEDURE: Bulk → Staging → Public
-- ===============================================

CREATE OR REPLACE PROCEDURE merge_bulk_to_final()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ===============================================
    -- PASO 1: Bulk → Staging
    -- ===============================================

    -- Clientes
    INSERT INTO staging.dim_cliente
    SELECT DISTINCT ON (customer_id) *
    FROM public.bulk_dim_cliente
    ORDER BY customer_id
    ON CONFLICT (customer_id) DO UPDATE
    SET customer_name = EXCLUDED.customer_name,
        segment       = EXCLUDED.segment,
        country       = EXCLUDED.country,
        city          = EXCLUDED.city,
        state         = EXCLUDED.state,
        postal_code   = EXCLUDED.postal_code,
        region        = EXCLUDED.region;

    -- Productos
    INSERT INTO staging.dim_producto
    SELECT DISTINCT ON (product_id) *
    FROM public.bulk_dim_producto
    ORDER BY product_id
    ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        category     = EXCLUDED.category,
        sub_category = EXCLUDED.sub_category;

    -- Ventas
    INSERT INTO staging.fact_ventas
    SELECT DISTINCT ON (order_id || '-' || product_id)
        order_id || '-' || product_id AS taligent_id,
        order_id,
        order_date,
        ship_date,
        customer_id,
        product_id,
        sales,
        quantity,
        discount,
        profit
    FROM public.bulk_fact_ventas
    ORDER BY order_id || '-' || product_id
    ON CONFLICT (taligent_id) DO UPDATE
    SET order_date  = EXCLUDED.order_date,
        ship_date   = EXCLUDED.ship_date,
        customer_id = EXCLUDED.customer_id,
        product_id  = EXCLUDED.product_id,
        sales       = EXCLUDED.sales,
        quantity    = EXCLUDED.quantity,
        discount    = EXCLUDED.discount,
        profit      = EXCLUDED.profit;

    -- Limpiar tablas bulk
    TRUNCATE public.bulk_dim_cliente, public.bulk_dim_producto, public.bulk_fact_ventas;

    -- ===============================================
    -- PASO 2: Staging → Public (finales)
    -- ===============================================

    -- Clientes
    INSERT INTO public.dim_cliente
    SELECT * FROM staging.dim_cliente
    ON CONFLICT (customer_id) DO UPDATE
    SET customer_name = EXCLUDED.customer_name,
        segment       = EXCLUDED.segment,
        country       = EXCLUDED.country,
        city          = EXCLUDED.city,
        state         = EXCLUDED.state,
        postal_code   = EXCLUDED.postal_code,
        region        = EXCLUDED.region;

    -- Productos
    INSERT INTO public.dim_producto
    SELECT * FROM staging.dim_producto
    ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        category     = EXCLUDED.category,
        sub_category = EXCLUDED.sub_category;

    -- Ventas
    INSERT INTO public.fact_ventas
    SELECT * FROM staging.fact_ventas
    ON CONFLICT (taligent_id) DO UPDATE
    SET order_date  = EXCLUDED.order_date,
        ship_date   = EXCLUDED.ship_date,
        customer_id = EXCLUDED.customer_id,
        product_id  = EXCLUDED.product_id,
        sales       = EXCLUDED.sales,
        quantity    = EXCLUDED.quantity,
        discount    = EXCLUDED.discount,
        profit      = EXCLUDED.profit;

    -- Limpiar staging
    TRUNCATE staging.dim_cliente, staging.dim_producto, staging.fact_ventas;
END;
$$;
