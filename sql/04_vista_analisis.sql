CREATE OR REPLACE VIEW public.vw_analisis AS
SELECT 
    f.order_id,
    f.order_date,
    f.sales,
    c.customer_name,
    p.product_name,
    p.category
FROM public.fact_ventas f
JOIN public.dim_cliente c ON f.customer_id = c.customer_id
JOIN public.dim_producto p ON f.product_id = p.product_id;
