# Superstore · Arquitectura Medallion en Azure Databricks

**Proyecto final · Ingeniería de Datos e IA con Databricks (Smart Data, Grupo 18)**
Autor: **Renzo Valencia Castillo** · Lima, Perú · Noviembre 2026

ETL en **PySpark** sobre **Azure Databricks + Unity Catalog** con arquitectura **Medallion** (Raw → Bronze → Silver → Golden). La conexión al Data Lake se hace con **Managed Identity**. El ETL se despliega de **desarrollo a producción con CI/CD en GitHub Actions** y sus datos se consumen desde un **AI/BI Dashboard, Genie, Azure SQL + Power BI y Delta Sharing**.

---

## 1. Resumen ejecutivo

> **Mensaje clave.** Con un pipeline reproducible y gobernado, convertimos 9,994 líneas de venta en 4 marts de negocio. Con ellos se puede ver **dónde se pierde margen**: subcategorías deficitarias, descuentos altos y devoluciones. Todo se despliega a producción sin intervención manual.

| | |
|---|---|
| **Situación** | Superstore vende en 4 regiones de EE. UU. y 3 categorías. Los datos llegan como archivos sueltos (CSV de órdenes, JSON de devoluciones, Parquet de gerentes) sin control de calidad ni historial. |
| **Complicación** | El área financiera no puede responder con confianza qué productos y descuentos destruyen margen, ni quién debe ver qué datos. Cada análisis se rehace a mano. |
| **Resolución** | Arquitectura medallion en Unity Catalog con reglas de calidad, auditoría por ejecución, permisos por rol y un workflow productivo que despliega y ejecuta GitHub Actions. Las tablas Golden alimentan dashboards, lenguaje natural (Genie), Power BI y un socio externo vía Delta Sharing. |

**KPIs del pipeline** *(última ejecución en producción)*

| Ventas totales | Utilidad | Margen | Filas Bronze → Silver | Rechazadas por calidad | Duración workflow |
|:---:|:---:|:---:|:---:|:---:|:---:|
| `TODO` | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |

---

## 2. Arquitectura

![Arquitectura](evidencias/00_arquitectura.png)

**Flujo:**
1. Los insumos se alojan en el contenedor **`raw`** de ADLS Gen2. No se usan DBFS ni Volúmenes.
2. Databricks accede al lake con la **Managed Identity** de un *Access Connector* (Storage Credential `cred_superstore_mi` → External Locations).
3. El workflow **`WF_SUPERSTORE_ETL`** ejecuta: `Preparación de Ambiente → 3 ingestas en paralelo → Transform → Load → Export Azure SQL → Grants`.
4. Las tablas Golden se consumen en **AI/BI Dashboard + Genie**, **Power BI** (vía Azure SQL) y **Delta Sharing**.
5. Todo cambio pasa por **GitHub** (rama `construccion` → PR → `main`). **GitHub Actions** despliega `/proceso` a producción y ejecuta el workflow.

**Aislamiento dev / prod:** ambos workspaces comparten el metastore de East US 2 y el Data Lake. Cada ambiente escribe en su **propio catálogo** (`superstore_dev` / `superstore_prod`) y en su **propio prefijo físico** (`/dev/…`, `/prod/…`). Solo hay **un clúster encendido**, el de producción; en dev se usa serverless.

---

## 3. Servicios utilizados

| # | Servicio | Rol en el proyecto |
|---|---|---|
| 1 | **Azure Data Lake Storage Gen2** | Capa raw y almacenamiento físico Delta de bronze/silver/golden |
| 2 | **Access Connector for Azure Databricks** | Managed Identity para acceder al lake (sin llaves ni secretos) |
| 3 | **Azure Databricks (2 workspaces)** | Desarrollo (`dbw-superstore-dev`) y producción (`dbw-superstore-prod`) |
| 4 | **Unity Catalog** | Storage credential, external locations, catálogos, esquemas, tablas, grants y linaje |
| 5 | **Databricks Workflows (Jobs)** | Orquestación del ETL en producción (DAG de 8 tareas) |
| 6 | **GitHub + GitHub Actions** | Repositorio, ramas y pipeline CI/CD dev → prod (YAML 3) |
| 7 | **Azure Key Vault** | Credenciales de Azure SQL vía secret scope `kv-superstore` |
| 8 | **Azure SQL Database** | Destino relacional de las tablas Golden para Power BI |
| 9 | **Databricks AI/BI Dashboard** | Dashboard sobre las tablas Golden |
| 10 | **Databricks Genie** | Consultas en lenguaje natural sobre Golden |
| 11 | **Power BI** | Reporte financiero (`dashboard/*.pbix`) |
| 12 | **Delta Sharing** | Publicación de KPIs a un socio externo (open sharing) |

---

## 4. Estructura del repositorio

```
superstore-databricks-medallion/
├── .github/workflows/      → deploy-databricks.yml  (CI/CD dev → prod, basado en YAML 3)
├── datasets/               → insumos del ETL (CSV, JSON, Parquet) + descripción de la fuente
├── PrepAmb/                → preparación de ambiente en SQL (external locations, catálogo, esquemas, tablas)
├── proceso/                → notebooks que se DESPLIEGAN y EJECUTAN en producción
│   ├── 1_Preparacion_Ambiente.py
│   ├── 2_Ingest_Orders.py · 2_Ingest_Returns.py · 2_Ingest_People.py
│   ├── 3_Transform.py
│   ├── 4_Load.py
│   ├── 5_Export_AzureSQL.py
│   └── 6_Grants.py
├── seguridad/              → grants a usuarios y grupos (.sql)
├── reversion/              → contingencia: drop de objetos lógicos, rutas físicas y workflow (.sql / .py)
├── delta-sharing/          → share + recipient sobre tablas Golden (.py)
├── dashboard/              → AI/BI dashboard (.lvdash.json + .png), Power BI (.pbix + .png)
├── certificaciones/        → credenciales Databricks Academy (.png + enlaces)
├── evidencias/             → capturas: servicios, ejecuciones del workflow y de GitHub Actions, arquitectura
└── README.md
```

---

## 5. Datasets (insumos)

Fuente: **Sample – Superstore** (dataset de muestra oficial de Tableau; las órdenes también están publicadas en Kaggle). Las tres entidades del Excel original se publican en la capa raw **en tres formatos distintos**. Detalle en [`datasets/README.md`](datasets/README.md).

| Insumo | Formato | Ruta raw | Filas | Descripción |
|---|---|---|---:|---|
| Órdenes | CSV | `raw/superstore/orders/` | 9,994 | Línea de venta 2018–2021: orden, cliente, producto, ventas, cantidad, descuento, utilidad |
| Devoluciones | JSON | `raw/superstore/returns/` | 800 (296 órdenes) | Órdenes devueltas (una fila por línea, con duplicados) |
| Gerentes regionales | Parquet | `raw/superstore/people/` | 4 | Gerente responsable por región |

---

## 6. Modelo de datos por capa

| Capa | Tabla | Descripción |
|---|---|---|
| Bronze | `bronze.orders` · `bronze.returns` · `bronze.people` | Datos tipados con trazabilidad (`source_file`, `ingestion_ts`) |
| Silver | `silver.ventas_detalle` | Línea de venta limpia y enriquecida, **particionada por año** |
| Silver | `silver.dim_cliente` · `silver.dim_producto` | Dimensiones conformadas |
| Golden | `golden.kpi_mensual` | Ventas, utilidad, margen, devoluciones, acumulado anual y crecimiento YoY |
| Golden | `golden.rentabilidad_subcategoria` | Ranking de utilidad por región y subcategorías deficitarias |
| Golden | `golden.clientes_pareto` | Participación acumulada y clasificación ABC de clientes |
| Golden | `golden.desempeno_envios` | Días de despacho (promedio y p90) y margen por modo de envío |
| Control | `control.auditoria_etl` · `control.rechazados` | Bitácora por `run_id` del job y registros que no pasan las reglas de calidad |

Todas las tablas se crean en **Preparación de Ambiente**. El ETL **solo escribe con `insertInto`**.

---

## 7. ETL en PySpark: técnicas aplicadas

| Técnica (vista en el curso) | Dónde |
|---|---|
| Esquemas explícitos `StructType` / `StructField` (CSV y JSON), lectura de Parquet | `2_Ingest_*` |
| Opciones de lectura (`header`, `quote`, `escape`, `multiLine`, `mode=PERMISSIVE`) | `2_Ingest_Orders`, `2_Ingest_Returns` |
| Columna oculta `_metadata.file_path` (trazabilidad) | `2_Ingest_*` |
| `select` / `alias` / `withColumnRenamed`, `trim`, `initcap`, `lower` | `2_Ingest_*`, `3_Transform` |
| Parseo de fechas multi-formato tolerante a ANSI (`try_to_timestamp`) | `2_Ingest_Orders` |
| Reglas de calidad declarativas + cuarentena | `2_Ingest_*` → `control.rechazados` |
| Deduplicación con `Window` + `row_number` | `3_Transform` |
| Manejo de nulos: `fillna`, `coalesce`, `dropDuplicates` | `3_Transform`, `2_Ingest_People` |
| **UDF de Python** (y su equivalente nativo `when/otherwise`) | `3_Transform` |
| **Broadcast join** y **left join** | `3_Transform` |
| `cache` / `persist` / `unpersist` | `3_Transform` |
| `repartition` por columna de partición + tabla particionada | `3_Transform` |
| Agregaciones `groupBy/agg`, `countDistinct`, `percentile_approx` | `4_Load` |
| Ventanas analíticas: suma acumulada, `lag` (YoY), `dense_rank`, `row_number` | `3_Transform`, `4_Load` |
| Escritura con `insertInto` (overwrite / append) | todos |
| Lectura y escritura JDBC (Azure SQL) | `5_Export_AzureSQL` |
| Parametrización con `dbutils.widgets` (`base_parameters` en el workflow) | todos |

---

## 8. Calidad de datos y auditoría

- **Reglas en Bronze** (orders): ids nulos, fechas inválidas, envío anterior a la orden, ventas/cantidad no positivas, descuento fuera de [0, 1], utilidad nula. Los registros inválidos se guardan en `control.rechazados` con el motivo.
- **Deduplicación en Silver** por `row_id`, quedándose con la versión más reciente.
- **Cuadre Silver = Golden:** si las ventas no cuadran, la tarea `Load` falla y el workflow se detiene.
- **Bitácora:** cada tabla cargada registra filas leídas, escritas y rechazadas en `control.auditoria_etl` con el `run_id` del job.

`TODO: captura de control.auditoria_etl y de control.rechazados`

---

## 9. Seguridad (Unity Catalog)

| Principal | Tipo | Permisos |
|---|---|---|
| `grp_ingenieros_datos` | Grupo | `USE CATALOG`, `USE SCHEMA` + `SELECT` + `MODIFY` en todas las capas, `READ FILES` en `exlt_raw` |
| `grp_analistas_finanzas` | Grupo | `USE CATALOG`, `USE SCHEMA` + `SELECT` en silver y golden |
| `grp_consumidores_bi` | Grupo | `USE CATALOG`, `USE SCHEMA` golden, `SELECT` en 3 tablas golden |
| `auditor.superstore@example.com` | Usuario | `SELECT` en `control.auditoria_etl` |

Usuarios y grupos ficticios creados en la Account Console y asignados a ambos workspaces. Script: `seguridad/01_Grants_Usuarios_Grupos.sql`. Se aplican automáticamente en la última tarea del workflow (`proceso/6_Grants`).

`TODO: captura SHOW GRANTS`

---

## 10. CI/CD (GitHub Actions)

1. Desarrollo en el **Git folder** del workspace dev, sobre la rama `construccion`.
2. *Commit & push* → **Pull Request** `construccion → main` → merge.
3. El merge dispara `.github/workflows/deploy-databricks.yml` (YAML 3 del curso), que:
   - exporta los notebooks de `/proceso` desde dev y **valida** cada exportación;
   - los importa en prod (`/Shared/superstore/proceso`);
   - elimina y vuelve a crear el workflow `WF_SUPERSTORE_ETL` sobre el clúster existente `cluster_prod_superstore`;
   - lo ejecuta con `env=prod` y lo monitorea hasta terminar.
4. Secrets del repositorio: `DATABRICKS_ORIGIN_HOST`, `DATABRICKS_ORIGIN_TOKEN`, `DATABRICKS_DEST_HOST`, `DATABRICKS_DEST_TOKEN`.

`TODO: captura de GitHub Actions en verde + workflow ejecutado en prod`

---

## 11. Visualización

`TODO: capturas del AI/BI Dashboard, Genie y Power BI`

---

## 12. Delta Sharing

Share `share_superstore_golden` (tablas `kpi_mensual` y `rentabilidad_subcategoria`, con historial) para el destinatario `rcp_socio_comercial` (open sharing). Ver `delta-sharing/01_Delta_Sharing.py`.

`TODO: captura`

---

## 13. Reversión (plan de contingencia)

Si un pase a producción falla, se ejecuta en orden:
1. `reversion/01_Drop_Objetos_Logicos.sql`: Delta Sharing, tablas, esquemas y catálogo.
2. `reversion/02_Drop_Rutas_Fisicas.py`: directorios Delta en ADLS (con modo simulación).
3. `reversion/03_Delete_Workflow_Notebooks.py`: elimina el job y los notebooks desplegados vía API (Databricks SDK).

---

## 14. Evidencias

`TODO: índice de capturas`

---

## 15. Certificaciones

`TODO`

---

## 16. Cómo reproducir

1. Aprovisionar: Resource Group, ADLS Gen2 (contenedores `raw`, `bronze`, `silver`, `golden`, `metastore`), Access Connector (rol *Storage Blob Data Contributor*) y 2 workspaces Premium en la misma región.
2. Crear la Storage Credential `cred_superstore_mi` con el Resource ID del Access Connector.
3. Subir `datasets/` a `raw/superstore/{orders,returns,people}/`.
4. En dev: clonar el repo como Git folder y ejecutar `proceso/1_…` a `6_…` con `env=dev`.
5. En prod: crear el clúster `cluster_prod_superstore`, configurar los 4 secrets en GitHub y hacer merge `construccion → main`.

---
<sub>Fuente de datos: dataset Superstore (Kaggle). Elaboración propia. Proyecto académico: datos ficticios, sin información confidencial.</sub>
