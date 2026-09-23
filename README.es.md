[English](README.md) | **Español**

<div align="center">

# 🥇 samegold

**Un cierre mensual de ingresos sobre Spark y Delta Lake que respalda cada número que publica con
evidencia que cualquiera puede recalcular.**

[![fast](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml)
[![spark](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml)
[![evidence](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml)
[![databricks](https://github.com/marcosmatalab/samegold/actions/workflows/databricks.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/databricks.yml)
[![release](https://img.shields.io/github/v/release/marcosmatalab/samegold)](https://github.com/marcosmatalab/samegold/releases/latest)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue)](LICENSE)

![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.2.0-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-4.4.0-00ADD4)
![Databricks](https://img.shields.io/badge/Databricks-Asset%20Bundles-FF3621?logo=databricks&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-reference%20engine-FFF000?logo=duckdb&logoColor=black)
![mypy](https://img.shields.io/badge/mypy-strict-2A6DB2)
![ruff](https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black)

🧾 cierre bitemporal · ⚖️ paridad entre motores · 🧬 pruebas de mutación · 💥 inyección de fallos · 🔒 purga de datos personales · 🔗 evidencia encadenada por hash

</div>

> [!TIP]
> **En una frase:** samegold cierra los ingresos mensuales de un negocio sobre Spark y Delta
> Lake, conserva cada versión ya firmada cuando las devoluciones tardías mueven un mes, contrasta
> el cierre con una referencia independiente que tiene que coincidir al céntimo, y publica cada
> claim a partir de mediciones encadenadas por hash que se pueden recalcular a demanda.

## 🎯 Qué hace

- 🧾 **Cierra el mes.** Ventas, devoluciones y rectificaciones fluyen por bronze → silver → gold
  sobre Delta Lake y terminan en un cierre mensual versionado e inmutable.
- ⏳ **Mantiene la historia exacta.** Una devolución se imputa al mes de la venta, así que un mes
  que finanzas ya ha firmado puede moverse. Cada versión cerrada se conserva junto a la que la
  sustituyó.
- ⚖️ **Lo contrasta con una referencia independiente.** El pipeline de Spark y Delta Lake y una
  referencia independiente en DuckDB tienen que producir un único digest canónico, y el despliegue
  en Databricks se contrasta con esa referencia al céntimo, versión a versión.
- 🔬 **Vuelve a medir sus propias afirmaciones.** Cada claim, de `SG-00` a `SG-09`, se vuelve a medir
  con semillas derivadas del sha del commit, se añade a un registro de evidencia encadenado por
  hash y se renderiza en esta página.

## 📊 Métricas clave

**Cada métrica de esta tabla se renderiza desde `evidence/`; ninguna está escrita a mano.** `make
readme` las escribe y `samegold check` hace fallar el build si alguna se aparta de su registro.

| | Qué se mide | Resultado | Claim |
|---|---|---|---|
| ✅ | Vía rápida, sin JVM y sin credenciales | <!--sg:SG-00.artifact.tests_passed-->737<!--/sg--> tests superados en <!--sg:SG-00.artifact.fast_lane_seconds-->79.5<!--/sg--> s | `SG-00` |
| ⚖️ | La referencia DuckDB y el libro mayor por construcción coinciden en cada cierre | <!--sg:SG-01.rate-->15/15 (95% CI 79.6%-100.0%)<!--/sg--> | `SG-01` |
| 🔁 | Reentregar todos los ficheros con otra ruta no cambia nada | <!--sg:SG-02.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg--> | `SG-02` |
| 🧬 | Mutantes SQL generados no equivalentes que el arnés mata | <!--sg:SG-03.rate-->67/67 (95% CI 94.6%-100.0%)<!--/sg--> | `SG-03` |
| ⏳ | Meses cerrados que se movieron tras la firma, con cada versión igual a la referencia | <!--sg:SG-04.rate-->2/2 (95% CI 34.2%-100.0%)<!--/sg--> | `SG-04` |
| 🧮 | Invariantes de dimensión y de conservación, sin oráculo | <!--sg:SG-05.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg--> | `SG-05` |
| 🔗 | Registros de evidencia verificados en la cadena de hashes | <!--sg:SG-06.artifact.records_verified-->251<!--/sg--> | `SG-06` |
| 💥 | Caídas inyectadas que el escritor de silver sobrevive | <!--sg:SG-07.rate-->20/20 (95% CI 83.9%-100.0%)<!--/sg--> | `SG-07` |
| 🔒 | Identificadores directos fuera de gold, con la purga verificada | <!--sg:SG-08.rate-->6/6 (95% CI 61.0%-100.0%)<!--/sg--> | `SG-08` |
| 📦 | Ficheros eliminados por la compactación | <!--sg:SG-09.artifact.files_removed_by_compaction_pct-->92.5<!--/sg-->% | `SG-09` |
| 📉 | Reducción de la parte de la tabla que lee una consulta por sku, gracias al clustering | <!--sg:SG-09.artifact.share_read_reduction_pct-->78.25<!--/sg-->% | `SG-09` |
| ☁️ | Eventos que cerró la vía de Databricks, con cada versión igual al céntimo a la vía de código abierto | <!--dbx:rows.bronze_events-->1883<!--/dbx--> eventos | `SG-DBX-01` |
| 🧱 | Arnés de verificación frente a código de plataforma, en líneas | <!--sg:SG-00.artifact.harness_lines-->23 967<!--/sg--> frente a <!--sg:SG-00.artifact.platform_lines-->7 567<!--/sg--> | `SG-00` |

## 🚀 Pruébalo en minutos

Sin cuenta, sin credenciales, sin más red que PyPI.

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make install
make demo                 # the close, and the month that moved after it was signed off
make fast                 # the whole fast lane, no JVM
make refute SEED=424242   # the seven data claims again, on a seed nobody chose
```

Lo que imprime `make demo`:

<!-- samegold:begin demo -->
```text
samegold demo - 772 events, 301 files, seed 3606824677207351751

  Month 2026-01 was closed at 2026-02-05 reporting 150 658,58 EUR of net revenue.
  By 2026-04-05, late returns and late amendments had moved it to 147 227,51 EUR.
  That is -3 431,07 EUR, -2.28% of a month that finance had already signed off.

  The customer dimension is well formed: yes.
  Two implementations of that number are compared on this data by `samegold evidence`.

  0.2s, no account, no credentials, nothing installed beyond this package.
```
<!-- samegold:end demo -->

**Esa salida se renderiza desde la evidencia, no está pegada,** así que siempre coincide con lo
que imprime el programa en el commit actual.

**El último comando es la clave.** Las semillas derivan del sha del commit, así que no se puede
elegir una favorable sin hacer un commit nuevo, que queda en el historial. `make refute` deja que
cualquiera elija la suya y ejecuta con ella las siete claims sobre los datos, lo que convierte
cada una en una invitación a refutarla.

![make refute con la semilla 424242: las siete claims sobre los datos ejecutadas otra vez, que se imprimen a medida que pasan](docs/img/refute.gif)

**Una grabación real, no una animación:** reproducida a 4x, sobre una ejecución real de 73,1 s.
[`docs/refute.tape`](docs/refute.tape) es el guion que ejecuta `vhs`, y `make gif` la vuelve a
grabar desde una ejecución nueva.

`make preflight` es la puerta antes de un push y `make doctor` informa de lo que esta máquina
puede ejecutar.

## 🏗️ Arquitectura

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/pipeline-dark.svg">
  <img alt="bronze_events hacia silver_classified, que se divide en silver_events y silver_quarantine; gold lee silver_classified para construir revenue_by_month y dim_customer_scd2; el close_month bitemporal escribe una versión inmutable por cierre en revenue_closed. La vía de Databricks se contrasta al céntimo con la referencia DuckDB." src="docs/img/pipeline-light.svg">
</picture>

**Un pipeline medallion con un contrato en la puerta.** Los eventos en bruto llegan a
`bronze_events`, `silver_classified` aplica el contrato de datos, los registros inválidos van a
`silver_quarantine`, y gold contiene `revenue_by_month`, una dimensión de clientes de Tipo 2
`dim_customer_scd2` y el cierre versionado `revenue_closed`.

**El diagrama se comprueba contra el código.**
`tests/fast/test_documentation.py::test_the_figures_agree_with_the_repository` deriva los nombres
de las tablas y las lecturas entre ellas parseando `databricks/src/`, y las cifras de dinero de
`evidence/databricks/SG-DBX-01.json`, así que una tabla renombrada o una flecha invertida hacen
fallar el build.

## ⏳ El cierre bitemporal

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/restatement-dark.svg">
  <img alt="Línea de tiempo: una venta el 5 de enero, enero cierra el 5 de febrero con un bruto de 14198046, una devolución de esa venta de enero llega el 18 de febrero, y el 5 de marzo enero se reexpresa como versión 1 mientras la versión 0 sigue sin cambios." src="docs/img/restatement-light.svg">
</picture>

Una devolución puede llegar hasta 45 días después de la venta y se imputa al mes de la **venta**. Por
eso el cierre sigue dos ejes de tiempo: cuándo ocurrió algo y cuándo se enteró el negocio. La
versión que firmó finanzas se queda exactamente como estaba, al lado de la que la sustituyó, y
`SG-04` mide cuánto se mueve cada mes cerrado y contrasta cada versión con la referencia DuckDB en
cada ejecución.

**Enero, cerrado tres veces en Databricks, sin reescribir ninguna versión:** un bruto de
14 198 046 céntimos en la firma, 25 582 615 tras los primeros eventos tardíos y
<!--dbx:revenue.2026_01.gross_cents-->37 622 605<!--/dbx--> tras los segundos, cada uno igual al
céntimo a la vía de código abierto. La vía se ejecutó de principio a fin en Databricks Free Edition.
[`docs/postmortem-2026-03-06.md`](docs/postmortem-2026-03-06.md) documenta la reexpresión como un
informe de incidente.

## 🔬 Cómo se demuestra cada número

```mermaid
flowchart TD
    SHA["commit sha"] --> SEEDS["deterministic seeds"]
    SEEDS --> GEN["generator: sales, returns, amendments"]
    GEN --> LEDGER["by-construction ledger"]
    GEN --> DUCK["DuckDB reference close"]
    GEN --> SPARK["Spark + Delta Lake<br/>bronze → silver → gold"]
    GEN --> ATTACK["mutation · crash injection<br/>privacy purge · layout cost"]
    LEDGER --> AGREE{"SG-01: agree to the cent<br/>at every close"}
    DUCK --> AGREE
    DUCK --> DIGEST{"same canonical digest<br/>spark + delta lanes"}
    SPARK --> DIGEST
    AGREE --> CLAIMS["claims SG-00 … SG-09"]
    ATTACK --> CLAIMS
    CLAIMS --> CHAIN[("evidence/history.jsonl<br/>hash chain")]
    CHAIN --> PAGE["README + CLAIMS.md<br/>make readme"]
    CHAIN --> VERIFY["samegold verify-latest<br/>recomputes each record"]
    classDef input fill:#1f6feb,stroke:#1f6feb,color:#ffffff
    classDef engine fill:#e25a1c,stroke:#e25a1c,color:#ffffff
    classDef proof fill:#2da44e,stroke:#2da44e,color:#ffffff
    class SHA,SEEDS,GEN input
    class SPARK,DUCK,LEDGER,ATTACK engine
    class AGREE,DIGEST,CLAIMS,CHAIN,PAGE,VERIFY proof
```

Las claims de abajo se renderizan desde `evidence/history.jsonl`, una cadena de hashes de
solo adición, a partir del registro más reciente de cada claim. Cuando la población cambia, las
claims se vuelven a ejecutar y se añade un registro nuevo, y los que ya están en la cadena se
quedan como están: **nunca se edita ni se reemplaza** un registro.
[ADR 0010](docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md) es la
política completa. La tabla se genera a partir de los registros, por eso aparece en inglés en las
dos portadas.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 737/737 (95% CI 99.5%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-03` mutation campaign | PASS | 67/67 (95% CI 94.6%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-06` the evidence chain verifies and every seed derives from its commit | PASS | 251/251 (95% CI 98.5%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-07` the silver writer survives a crash at each of its structural points | PASS | 20/20 (95% CI 83.9%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-08` no direct identifier reaches gold, and a purge really purges | PASS | 6/6 (95% CI 61.0%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |
| `SG-09` what layout costs, in files and bytes | PASS | 5/5 (95% CI 56.6%-100.0%) | oss-local | [CI, d69164100](https://github.com/marcosmatalab/samegold/actions/runs/35782684308) |

<!-- samegold:end claims -->

**Cada fila enlaza la ejecución de CI que la produjo, y `samegold verify-latest` recalcula cada
una desde las semillas que nombra su propio registro.** Un registro bien formado no basta: el
número se tiene que reproducir. [ADR 0011](docs/adr/0011-the-gate-recomputes-the-record.md)
documenta la puerta de recálculo.

## ☁️ La vía de Databricks

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/job-graph-dark.svg">
  <img alt="El job de cierre mensual de samegold: ingest_and_transform ejecuta el pipeline de Lakeflow, close_month escribe el cierre versionado, y la tarea condicional did_the_close_restate manda un cierre que reexpresó un mes a verify_each_restated_month, una vez por mes, y un cierre que no reexpresó nada a verify_no_restatement. publish_evidence corre después de cualquiera de las dos." src="docs/img/job-graph-light.svg">
</picture>

**Desplegada y ejecutada de principio a fin en un workspace real de Databricks.** Un Databricks
Asset Bundle despliega un pipeline de Lakeflow y un job de cierre mensual cuya tarea condicional
decide la rama según si el cierre reexpresó un mes. Los registros de ejecución están versionados
en [`evidence/databricks/`](evidence/databricks/) con sus ids de job run, de pipeline y de
update, y [`docs/databricks-run-evidence.md`](docs/databricks-run-evidence.md) renderiza lo que
midió el workspace: la dimensión de Tipo 2 fila a fila con su `__START_AT` y su `__END_AT`, las
versiones cerradas, las expectativas con sus recuentos de aciertos y fallos, y cada evento que el
contrato rechazó.

**Verificado desde un clon, sin cuenta:**
<!--sg:SG-00.artifact.tests_databricks_bundle-->135<!--/sg--> tests ejercitan `databricks/` y
`scripts/databricks_run.sh` contra una CLI simulada en el `PATH`: cada ruta de notebook, cada
widget y cada parámetro del job, el techo de tareas concurrentes calculado a partir del grafo de
dependencias de arriba, y la comprobación que se niega a ejecutar un job desplegado desde un commit que
no es `HEAD`.

**Seguro por diseño.** `.github/workflows/databricks.yml` solo acepta `workflow_dispatch`, usa
`validate` por defecto, no arranca compute, fija cada action a un sha de commit y lee su token de
un entorno de GitHub en lugar de un secreto del repositorio; como no tiene disparador
`pull_request`, el pull request de un fork no puede alcanzarlo.

## 🧰 Stack técnico

| Capa | Tecnología |
|---|---|
| ⚙️ Procesamiento | PySpark 4.2.0 · Delta Lake 4.4.0 · delta-rs 1.6 · arquitectura medallion · SCD de Tipo 2 |
| ☁️ Nube | Databricks Asset Bundles · pipelines de Lakeflow · Jobs con tareas condicionales · Unity Catalog |
| 🦆 Motor de referencia | DuckDB, que calcula el mismo cierre de forma independiente |
| 🧪 Verificación | pytest · Hypothesis · mutantes SQL generados con sqlglot · inyección de fallos · intervalos de Wilson al 95% |
| 🔗 Evidencia | cadena de hashes JSONL de solo adición · semillas derivadas del sha del commit |
| 🛠️ Calidad | Python 3.11+ · ruff · mypy strict · GitHub Actions: fast, spark, evidence, databricks |

## 🗺️ Documentación

- [`docs/how-it-works.md`](docs/how-it-works.md) - el diseño: tres testigos, el digest, la puerta de evidencia, lo que cuesta el layout
- [`CLAIMS.md`](CLAIMS.md) - cada claim, su experimento y su alcance
- [`FINDINGS.md`](FINDINGS.md) - el registro de revisión adversarial: cada defecto que cazó el arnés, ordenado por lo que enseña
- [`CHANGELOG.md`](CHANGELOG.md) - qué cambió cada versión
- [`docs/adr/`](docs/adr/) - las decisiones de arquitectura, cada una con la alternativa que rechazó y por qué
- [`docs/databricks-run.md`](docs/databricks-run.md) - qué despliega la vía de la nube y qué ejecutó
- [`docs/databricks-run-evidence.md`](docs/databricks-run-evidence.md) - qué midió el workspace, renderizado desde los registros que dejó
- [`docs/runbook.md`](docs/runbook.md) - runbook de guardia: qué significa una alerta, problema de datos o de plataforma, y cómo reparar una ejecución
- [`docs/findings/`](docs/findings/) - los informes de lo que encontró la puerta de recálculo
- [`docs/join-skew.md`](docs/join-skew.md) - una clave se lleva el 30% de las filas: qué le pasa al join, medido
- [`EXAM_MAP.md`](EXAM_MAP.md) - la guía de Databricks Professional, objetivo por objetivo
- [`PARITY.md`](PARITY.md) - la vía de código abierto frente a Databricks, claim a claim
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - cómo contribuir: `make preflight` es la puerta
- [`docs/limits.md`](docs/limits.md) - limitaciones conocidas y riesgo residual

Apache-2.0.
