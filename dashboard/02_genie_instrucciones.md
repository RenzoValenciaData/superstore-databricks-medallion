# Genie · "Superstore · Rentabilidad"

Espacio de Genie (preguntas en lenguaje natural sobre las tablas del lakehouse) creado en el workspace **prod**.

## Tablas del espacio

| Tabla | Uso |
|---|---|
| `superstore_prod.golden.kpi_mensual` | Tendencias mensuales y anuales, región, categoría, crecimiento YoY |
| `superstore_prod.golden.rentabilidad_subcategoria` | Utilidad y margen por subcategoría y región |
| `superstore_prod.golden.clientes_pareto` | Concentración de clientes (clase ABC) |
| `superstore_prod.golden.desempeno_envios` | Días de envío por modo y región |
| `superstore_prod.silver.ventas_detalle` | Detalle por línea (banda de descuento, devoluciones) |

## Instrucciones generales (se pegan en *Instructions*)

```text
Eres un analista financiero de Superstore (retail de muebles, tecnología y artículos de oficina en EE. UU.). Responde siempre en español.
- Montos en dólares (USD). El período disponible es 2018–2021.
- El margen se calcula como SUM(utilidad) / SUM(ventas). Nunca promedies la columna margen_pct entre filas.
- Para tendencias por mes, año, región o categoría usa golden.kpi_mensual.
- Para rentabilidad por subcategoría usa golden.rentabilidad_subcategoria. Una subcategoría es deficitaria si su utilidad es negativa.
- Para el impacto de los descuentos usa silver.ventas_detalle y la columna banda_descuento.
- Para la concentración de clientes usa golden.clientes_pareto. La clase A agrupa a los clientes que explican el 80 % de las ventas.
- Para logística usa golden.desempeno_envios.
- Una orden devuelta tiene devuelto = true en silver.ventas_detalle.
```

## Preguntas de ejemplo (*Sample questions*)

1. ¿Cuál fue el margen por región en 2021?
2. ¿Qué subcategorías tienen utilidad negativa y cuánto pierden?
3. ¿Cuánta utilidad se pierde en las ventas con descuento mayor al 20 %?
4. ¿Cuántos clientes son clase A y qué porcentaje de las ventas explican?
5. ¿Cómo crecieron las ventas año contra año?

## Respuestas esperadas (para validar que Genie responde bien)

| Pregunta | Resultado correcto |
|---|---|
| 1 | West 17.5 %, East 15.6 %, South 7.2 %, Central 5.1 % |
| 2 | Tables (−US$17.7 mil), Bookcases (−US$3.5 mil), Supplies (−US$1.2 mil) |
| 3 | Medio (21–40 %): −US$35.8 mil; Alto (> 40 %): −US$99.6 mil. Total: −US$135.4 mil |
| 4 | 395 clientes, 80 % de las ventas |
| 5 | 2019: −2.8 %; 2020: +29.5 %; 2021: +20.4 % |
