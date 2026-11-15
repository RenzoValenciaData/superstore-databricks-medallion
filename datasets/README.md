# Datasets (insumos del ETL)

**Fuente:** *Sample – Superstore*, dataset de muestra oficial de **Tableau** (ventas minoristas de una cadena en EE. UU.).
- La tabla de órdenes también está publicada en Kaggle (*Supermarket/Superstore dataset bundle*, archivo `Sample - Superstore.csv`).
- Se usa la versión Excel de Tableau (`Sample - Superstore.xls`) porque trae las **3 entidades relacionadas**: `Orders`, `Returns` y `People`. El CSV de Kaggle solo trae `Orders`.

| Archivo | Entidad | Formato | Filas | Ruta en la capa raw (ADLS) | Llave de unión |
|---|---|---|---:|---|---|
| `orders.csv` | Órdenes (línea de venta) | CSV | 9,994 | `raw/superstore/orders/` | — |
| `returns.json` | Devoluciones | JSON | 800 (296 órdenes únicas) | `raw/superstore/returns/` | `Order ID` |
| `people.parquet` | Gerentes regionales | Parquet | 4 | `raw/superstore/people/` | `Region` |

**Preparación aplicada** (cada hoja del Excel se exportó a un formato distinto para demostrar la ingesta multi-formato):
- Las fechas se exportaron como texto `M/d/yyyy`, que es el formato del CSV original.
- `Postal Code` se exportó como texto, conservando los ceros a la izquierda. 11 registros de Burlington (Vermont) vienen **sin código postal** en la fuente; el ETL los trata en Silver.
- Ventas y utilidad se redondearon a 4 decimales para eliminar los artefactos de punto flotante del Excel. Los totales no cambian: **ventas US$2,297,200.86** y **utilidad US$286,397.02**.
- `returns.json` trae una fila por línea de pedido devuelta, así que hay órdenes repetidas. La deduplicación se hace en el ETL (Silver).
