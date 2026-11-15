-- =====================================================================================
-- Datasets del AI/BI Dashboard "Superstore · Rentabilidad" (Databricks SQL)
-- Fuente: superstore_prod (tablas golden y silver generadas por el ETL en PySpark)
-- Cada consulta se pega en el Dashboard → pestaña Data → "Create from SQL"
-- =====================================================================================

-- ds_kpis · Tarjetas KPI (ventas, utilidad, margen, órdenes, tasa de devolución)
SELECT
  ROUND(SUM(ventas), 0)                                                    AS ventas,
  ROUND(SUM(utilidad), 0)                                                  AS utilidad,
  ROUND(SUM(utilidad) / SUM(ventas), 4)                                    AS margen,
  COUNT(DISTINCT id_orden)                                                 AS ordenes,
  ROUND(COUNT(DISTINCT CASE WHEN devuelto THEN id_orden END)
        / COUNT(DISTINCT id_orden), 4)                                     AS tasa_devolucion,
  ROUND(SUM(monto_descuento), 0)                                           AS monto_descuento
FROM superstore_prod.silver.ventas_detalle;

-- ds_mensual · Tendencia mensual de ventas, utilidad y margen
SELECT
  anio_mes,
  ROUND(SUM(ventas), 2)                         AS ventas,
  ROUND(SUM(utilidad), 2)                       AS utilidad,
  ROUND(SUM(utilidad) / SUM(ventas), 4)         AS margen
FROM superstore_prod.golden.kpi_mensual
GROUP BY anio_mes
ORDER BY anio_mes;

-- ds_anual · Crecimiento anual y margen
SELECT
  anio,
  ROUND(SUM(ventas), 0)                         AS ventas,
  ROUND(SUM(utilidad), 0)                       AS utilidad,
  ROUND(SUM(utilidad) / SUM(ventas), 4)         AS margen
FROM superstore_prod.golden.kpi_mensual
GROUP BY anio
ORDER BY anio;

-- ds_region · Ventas y margen por región
SELECT
  region,
  ROUND(SUM(ventas), 0)                         AS ventas,
  ROUND(SUM(utilidad), 0)                       AS utilidad,
  ROUND(SUM(utilidad) / SUM(ventas), 4)         AS margen
FROM superstore_prod.golden.kpi_mensual
GROUP BY region
ORDER BY margen DESC;

-- ds_subcategoria · Utilidad por subcategoría (las deficitarias quedan arriba)
SELECT
  categoria,
  subcategoria,
  ROUND(SUM(ventas), 0)                         AS ventas,
  ROUND(SUM(utilidad), 0)                       AS utilidad,
  ROUND(SUM(utilidad) / SUM(ventas), 4)         AS margen,
  ROUND(SUM(monto_descuento), 0)                AS monto_descuento
FROM superstore_prod.golden.rentabilidad_subcategoria
GROUP BY categoria, subcategoria
ORDER BY utilidad ASC;

-- ds_descuento · Impacto de la banda de descuento en la utilidad
SELECT
  banda_descuento,
  COUNT(*)                                      AS lineas,
  ROUND(SUM(ventas), 0)                         AS ventas,
  ROUND(SUM(utilidad), 0)                       AS utilidad,
  ROUND(SUM(utilidad) / SUM(ventas), 4)         AS margen
FROM superstore_prod.silver.ventas_detalle
GROUP BY banda_descuento
ORDER BY utilidad DESC;

-- ds_clientes_abc · Concentración de clientes (Pareto)
SELECT
  clase_abc,
  COUNT(*)                                      AS clientes,
  ROUND(SUM(ventas), 0)                         AS ventas,
  ROUND(SUM(ventas) / SUM(SUM(ventas)) OVER (), 4) AS participacion
FROM superstore_prod.golden.clientes_pareto
GROUP BY clase_abc
ORDER BY clase_abc;

-- ds_envios · Desempeño logístico por modo de envío
SELECT
  modo_envio,
  SUM(ordenes)                                  AS ordenes,
  ROUND(SUM(dias_envio_prom * ordenes) / SUM(ordenes), 2) AS dias_envio_prom,
  MAX(dias_envio_p90)                           AS dias_envio_p90,
  ROUND(SUM(utilidad) / SUM(ventas), 4)         AS margen
FROM superstore_prod.golden.desempeno_envios
GROUP BY modo_envio
ORDER BY dias_envio_prom;
