# Databricks notebook source
# MAGIC %md
# MAGIC # 5 · Export Golden → Azure SQL Database (destino de servicio)
# MAGIC
# MAGIC Publica las tablas Golden en **Azure SQL Database** vía JDBC, para que Power BI (u otra app) consuma un modelo
# MAGIC relacional. Las credenciales **no están en el código**: se leen de un *secret scope* respaldado por **Azure Key Vault**.
# MAGIC
# MAGIC | Secreto (Key Vault) | Contenido |
# MAGIC |---|---|
# MAGIC | `sql-server` | `<servidor>.database.windows.net` |
# MAGIC | `sql-database` | nombre de la base de datos |
# MAGIC | `sql-user` | usuario SQL |
# MAGIC | `sql-password` | contraseña |
# MAGIC
# MAGIC Si el secret scope no existe en el workspace, el notebook termina en **SKIPPED** sin romper el workflow.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Ambiente")
dbutils.widgets.text("secretScope", "kv-superstore", "Secret scope (Key Vault)")
dbutils.widgets.text("run_id", "", "Run ID (lo envía el workflow)")

# COMMAND ----------

env = dbutils.widgets.get("env")
scope = dbutils.widgets.get("secretScope")
run_id = dbutils.widgets.get("run_id") or f"manual-{uuid.uuid4().hex[:8]}"
catalogo = f"superstore_{env}"
inicio = datetime.now()

scopes_disponibles = [s.name for s in dbutils.secrets.listScopes()]
if scope not in scopes_disponibles:
    print(f"Secret scope '{scope}' no existe en este workspace -> exportación omitida")
    dbutils.notebook.exit("SKIPPED - sin secret scope")

# COMMAND ----------

servidor = dbutils.secrets.get(scope, "sql-server")
base_datos = dbutils.secrets.get(scope, "sql-database")
usuario = dbutils.secrets.get(scope, "sql-user")
clave = dbutils.secrets.get(scope, "sql-password")

jdbc_url = (f"jdbc:sqlserver://{servidor}:1433;database={base_datos};encrypt=true;"
            "trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# COMMAND ----------

def auditar(proceso, tabla, leidas, escritas, rechazadas=0, estado="OK"):
    tabla_audit = f"{catalogo}.control.auditoria_etl"
    fila = [(run_id, env, proceso, tabla, int(leidas), int(escritas), int(rechazadas),
             inicio, datetime.now(), estado)]
    spark.createDataFrame(fila, spark.table(tabla_audit).schema) \
         .write.mode("append").insertInto(tabla_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exportación (overwrite + truncate para conservar la estructura de la tabla en SQL)

# COMMAND ----------

tablas = ["kpi_mensual", "rentabilidad_subcategoria", "clientes_pareto", "desempeno_envios"]

for t in tablas:
    df = spark.table(f"{catalogo}.golden.{t}").withColumn("ambiente", F.lit(env))
    filas = df.count()
    (df.write
       .format("jdbc")
       .option("url", jdbc_url)
       .option("dbtable", f"dbo.{t}")
       .option("user", usuario)
       .option("password", clave)
       .option("truncate", "true")
       .mode("overwrite")
       .save())
    auditar("5_Export_AzureSQL", f"azuresql.dbo.{t}", filas, filas)
    print(f"✔ dbo.{t}: {filas} filas exportadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación: lectura de vuelta desde Azure SQL

# COMMAND ----------

df_check = (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "dbo.kpi_mensual")
            .option("user", usuario)
            .option("password", clave)
            .load())
print(f"dbo.kpi_mensual en Azure SQL: {df_check.count()} filas")
display(df_check.limit(10))

dbutils.notebook.exit("OK export Azure SQL")
