[English](README.md) | **Español**

# samegold

**Cómo se demuestra que un pipeline de datos hace lo que dice.** El pipeline es un cierre
mensual bitemporal sobre Delta Lake y Spark, calculado tres veces por tres motores. El resto es
un arnés cuyo único trabajo es demostrar que está mal, y el registro de cada vez que lo logró.

[![fast](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml)
[![spark](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml)
[![evidence](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml)
[![release](https://img.shields.io/github/v/release/marcosmatalab/samegold)](https://github.com/marcosmatalab/samegold/releases/latest)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue)](LICENSE)

**Existe porque un pipeline en verde publicó 2.767e19 céntimos de ingresos de enero,** seis
millones y medio de veces lo que su propio contrato permite para las líneas implicadas, **y
ninguno de los tres motores lo vio.** Dos implementaciones coincidían entre sí y con un libro
mayor correcto por construcción. Una campaña de mutación mató todos los mutantes que no había
clasificado como equivalentes. Dieciséis rondas de revisión adversarial no habían encontrado
nada. Tres eventos que el generador emite *para que sean rechazados* se contabilizaron como
ingresos, porque la clasificación leyó "no puedo responder" como "acepta".

Así que esto no es un pipeline que funcione. Es uno cuyas afirmaciones sobre sí mismo son
comprobables, con un registro de cada vez que una de ellas resultó ser falsa.

## Compruébalo tú mismo, en siete minutos

Sin cuenta, sin credenciales, sin más red que PyPI.

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make install
make demo                 # the close, and the month that moved after it was signed off
make fast                 # the whole fast lane, no JVM
make refute SEED=424242   # every claim again, on a seed nobody chose
```

Unos siete minutos de principio a fin sobre un clon limpio, y la mayor parte es el último
comando. En esta página no hay escrita a mano ninguna duración: `make doctor` imprime lo que ha
tardado tu máquina, y el tiempo de la propia vía es la cifra anclada de abajo. Lo que imprime
`make demo`:

<!-- samegold:begin demo -->
```text
samegold demo - 779 events, 312 files, seed 3866732607104142982

  Month 2026-01 was closed at 2026-02-05 reporting 146 712,77 EUR of net revenue.
  By 2026-04-05, late returns and late amendments had moved it to 141 620,94 EUR.
  That is -5 091,83 EUR, -3.47% of a month that finance had already signed off.

  The customer dimension is well formed: yes.
  Two implementations of that number are compared on this data by `samegold evidence`.

  0.2s, no account, no credentials, nothing installed beyond this package.
```
<!-- samegold:end demo -->

**Ese bloque se renderiza desde la evidencia, no está pegado.** Antes lo estaba, y era el único
defecto de esta página que un revisor podía encontrar ejecutando el comando que esta misma
sección le dice que ejecute: anunciaba 780 eventos donde el programa imprimía 694, y llevaba
dieciocho commits equivocado, porque las semillas derivan del sha del commit y un transcript
está caducado en el instante en que se copia.

**El último comando es el meollo.** Que las semillas deriven del sha del commit significa que
elegir una favorable exige cambiar el código, lo cual cambia la semilla. `make refute` te deja
elegirla de todas formas, y la cadena rechaza el resultado como evidencia. **Una claim que falla
con tu semilla es la incidencia más útil que alguien puede abrir aquí.**

![make refute con la semilla 424242: las siete claims sobre los datos ejecutadas otra vez, impresas según cada una pasa](docs/img/refute.gif)

**Ese GIF es una grabación, no un dibujo.** [`docs/refute.tape`](docs/refute.tape) es el guion
que ejecuta `vhs`, `make gif` lo regenera, y lo que pasa por pantalla es la salida del propio
programa a la velocidad a la que salió. Uno dibujado sería el transcript que esta página ya
quitó una vez, en color.

`make fast` es toda la vía rápida (<!--sg:SG-00.artifact.tests_fast-->719<!--/sg--> tests en
<!--sg:SG-00.artifact.fast_lane_seconds-->58.1<!--/sg--> s, sin JVM, sin credenciales), `make
preflight` la puerta antes de un push, `make doctor` lo que esta máquina puede ejecutar.

## Qué calcula

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/pipeline-dark.svg">
  <img alt="bronze_events hacia silver_classified, que se divide en silver_events y silver_quarantine; silver_events alimenta revenue_by_month y dim_customer_scd2; el close_month bitemporal escribe una versión inmutable por cierre en revenue_closed. La vía de Databricks y la referencia DuckDB lo calculan dos veces y tienen que coincidir en un único digest canónico." src="docs/img/pipeline-light.svg">
</picture>

**Ese diagrama se comprueba contra el código, de tres maneras.**
`tests/fast/test_documentation.py::test_the_figures_agree_with_the_repository` deriva los
nombres de las tablas y las lecturas entre ellas parseando `databricks/src/`, y las cifras de
dinero de `evidence/databricks/SG-DBX-01.json`. Una tabla renombrada, una flecha invertida o un
dígito cambiado lo ponen rojo, y cada uno lo pone rojo por su cuenta.

Se lo ganó en la primera ejecución. El dibujo trazaba la cadena ordenada de bronze a classified
a events a gold, y la vía no hace eso: gold lee `silver_classified`, cosa que el código dice en
una línea de docstring y el dibujo contradecía. Un diagrama que deriva es la misma clase de
defecto que una frase que deriva.

## Por qué un cierre necesita dos ejes de tiempo, en una imagen

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/restatement-dark.svg">
  <img alt="Línea de tiempo: una venta el 5 de enero, enero cierra el 5 de febrero con un bruto de 14198046, una devolución de esa venta de enero llega el 18 de febrero, y el 5 de marzo enero se reexpresa como versión 1 mientras la versión 0 sigue sin cambios." src="docs/img/restatement-light.svg">
</picture>

Una devolución se imputa al mes de la **venta**, no al mes en que llegó. Así que un mes que
finanzas ya ha dado por bueno puede moverse después, y la versión que firmaron tiene que seguir
ahí, sin cambios, al lado de la que la reemplazó. Eso es lo que compra "bitemporal", y es la
propiedad que mide `SG-04`.

## De qué está hecho sobre todo este repositorio, ya que alguien iba a contarlo

**El <!--sg:SG-00.artifact.platform_share_pct-->24.4<!--/sg-->% del código es Spark, Delta y
Databricks. El <!--sg:SG-00.artifact.harness_share_pct-->75.6<!--/sg-->% es el arnés que intenta
romperlo.** En líneas: <!--sg:SG-00.artifact.platform_lines-->7 567<!--/sg--> frente a
<!--sg:SG-00.artifact.harness_lines-->23 448<!--/sg-->. De los
<!--sg:SG-00.artifact.tests_fast-->719<!--/sg--> tests de la vía rápida,
<!--sg:SG-00.artifact.fast_lane_domain_tests-->175<!--/sg--> ejercitan el pipeline y
<!--sg:SG-00.artifact.fast_lane_repository_tests-->544<!--/sg--> comprueban las afirmaciones del
propio repositorio sobre sí mismo - un test del cierre, frente a un test del papeleo del propio
repositorio.

Esas cifras las mide `make evidence` y se renderizan aquí, como cualquier otro número de esta
página. El reparto es una suma sobre una clasificación que es un JUICIO, así que la
clasificación está escrita en `src/samegold/evidence/lane_split.py`, donde se puede discutir, y
<!--sg:SG-00.artifact.fast_lane_unclassified_files-->0<!--/sg--> ficheros de test quedan en
ninguna de las dos clases - un fichero que nadie ha clasificado hace fallar la vía rápida en vez
de que se le asigne un lado, porque la suma publicada es esa suma.

**Esa proporción es el meollo, no un accidente.** Cualquiera sabe escribir un cierre mensual. Lo
difícil, y de lo que esto va, es saber si el que has escrito está bien, y poder darle a otro un
comando que lo responda. El pipeline es el sujeto. El arnés es el trabajo.

Si has venido por Spark y Delta en concreto: `src/samegold/pipelines/`, `databricks/src/`,
`tests/spark/` y `tests/delta/`, y la vía de Databricks de más abajo corrió contra un workspace
de verdad.

## Las claims

Renderizadas desde `evidence/history.jsonl`, una cadena de hashes de sólo-añadir, a partir del
registro más reciente de cada claim. Cuando la población se mueve la regla es: ejecutar las
claims otra vez y añadir un registro nuevo; **nunca editar ni reemplazar** los que ya están en
la cadena. [ADR 0010](docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md)
es la política completa. La tabla está generada, así que es la misma tabla en las dos portadas y
en el idioma en el que están escritos los registros.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 703/703 (95% CI 99.5%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-03` mutation campaign | PASS | 67/67 (95% CI 94.6%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-06` the evidence chain verifies and every seed derives from its commit | PASS | 231/231 (95% CI 98.4%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-07` the silver writer survives a crash at each of its structural points | PASS | 20/20 (95% CI 83.9%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-08` no direct identifier reaches gold, and a purge really purges | PASS | 6/6 (95% CI 61.0%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |
| `SG-09` what layout costs, in files and bytes | PASS | 5/5 (95% CI 56.6%-100.0%) | oss-local | [CI, dbf5198bb](https://github.com/marcosmatalab/samegold/actions/runs/35740196795) |

<!-- samegold:end claims -->

Cada fila enlaza la ejecución del workflow que la produjo, y `samegold verify-latest` recalcula
cada una desde las semillas que nombra su propio registro. Ese comando existe porque las cuatro
defensas anteriores respondían todas a "¿está bien formado este registro?" y ninguna respondía a
"¿es cierto este número?": una revisión adversarial puso una puntuación de mutación perfecta en
esta página añadiendo un único registro bien formado, con semillas de verdad y un hash calculado
por el propio hasher de este repositorio, y `samegold check` salió con 0 con toda la vía rápida
en verde. [ADR 0011](docs/adr/0011-the-gate-recomputes-the-record.md) es el arreglo, las dos
claims que no recalcula por defecto, y el agujero que aun así deja.

## El mes que cerró dos veces

Una devolución puede llegar 45 días después de la venta y se imputa al mes de la **venta**, así
que un mes que finanzas dio por bueno puede moverse. Enero se movió: 14 198 046 céntimos en la
firma, reexpresado a 25 582 615 cuando llegaron eventos tardíos, y a
<!--dbx:revenue.2026_01.gross_cents-->37 622 605<!--/dbx--> cuando llegaron más - tres versiones,
ninguna de ellas reescrita. Esa última es la vía de Databricks, desplegada y ejecutada de punta a
punta en Free Edition sobre <!--dbx:rows.bronze_events-->1883<!--/dbx--> eventos, y coincide con
la vía de código abierto **al céntimo** - que lo calcula sin workspace ninguno.
[`docs/postmortem-2026-03-06.md`](docs/postmortem-2026-03-06.md) escribe la reexpresión como un
incidente; las cifras de la nube están ancladas a `evidence/databricks/SG-DBX-01.json` y se
comprueban en cada ejecución de la vía rápida.

## La vía de Databricks: qué puedes comprobar y qué te tienes que creer

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/job-graph-dark.svg">
  <img alt="El job de cierre mensual de samegold: ingest_and_transform ejecuta el pipeline de Lakeflow, close_month escribe el cierre versionado, y la tarea condicional did_the_close_restate manda un cierre que reexpresó un mes a verify_each_restated_month, una vez por mes, y un cierre que no reexpresó nada a verify_no_restatement. publish_evidence corre después de cualquiera de las dos." src="docs/img/job-graph-light.svg">
</picture>

**El bundle se comprueba desde un clon; el workspace no.** Son afirmaciones distintas, y
mezclarlas es como un repositorio acaba sonando mejor de lo que es.

**Comprobable aquí, sin cuenta:**
<!--sg:SG-00.artifact.tests_databricks_bundle-->126<!--/sg--> tests conducen `databricks/` y
`scripts/databricks_run.sh` contra una CLI de pega en el `PATH` - cada ruta de notebook, cada
widget, cada parámetro del job, el techo de tareas concurrentes calculado como la anchura del
grafo de dependencias de arriba, y la guarda que se niega a ejecutar un job desplegado desde un
commit que no es `HEAD`. La figura de arriba se comprueba contra
`databricks/resources/jobs.yml` mediante `tests/fast/test_documentation.py`: una tarea renombrada
en el YAML la pone roja.

**No comprobable aquí:** el workspace. La vía corrió cuatro veces entre el 3 y el 6 de septiembre
de 2026 contra un workspace de Databricks Free Edition, y los registros están commiteados en
[`evidence/databricks/`](evidence/databricks/) con sus ids de job run, de pipeline y de update.
**Este repositorio no guarda credenciales para él** - medido: cero secretos de repositorio, cero
entornos, y `databricks.yml` es sólo `workflow_dispatch` - así que nadie con un clon puede
volver a ejecutarla. Esos registros son lo único aquí que te tienes que creer, y lo dicen ellos
mismos: `"chain": {"chained": false}`.

Lo que eso compra en lugar de una captura de pantalla:
[**`docs/databricks-run-evidence.md`**](docs/databricks-run-evidence.md) renderiza lo que midió
el workspace a partir de esos registros - la dimensión de Tipo 2 fila a fila con su `__START_AT`
y su `__END_AT`, las cuatro versiones cerradas de dos meses, las expectativas con sus recuentos
de aciertos y fallos, los cuatro eventos que el contrato rechazó con el valor que lo provocó.
`samegold check` falla si una cifra de ahí deja de coincidir con los registros.

## Adónde ir después

- [`FINDINGS.md`](FINDINGS.md) - cada defecto que este repositorio encontró en sí mismo, por lo que enseña
- [`CLAIMS.md`](CLAIMS.md) - cada claim, su experimento, y lo que **no** demuestra
- [`CHANGELOG.md`](CHANGELOG.md) - qué cambió cada versión
- [`docs/how-it-works.md`](docs/how-it-works.md) - el diseño: tres testigos, el digest, la puerta de evidencia, lo que cuesta el layout
- [`docs/adr/`](docs/adr/) - las decisiones, cada una con la alternativa que rechazó y por qué
- [`docs/databricks-run.md`](docs/databricks-run.md) - qué despliega la vía de la nube y qué ejecutó
- [`docs/databricks-run-evidence.md`](docs/databricks-run-evidence.md) - qué midió el workspace, renderizado desde los registros que dejó
- [`docs/runbook.md`](docs/runbook.md) - la alerta ha sonado a las tres de la mañana: qué significa, problema de datos o de plataforma, y cómo reparar una ejecución sin gastar la cuota del día
- [`docs/findings/`](docs/findings/) - los informes: qué encontró la puerta de recálculo en su primera ejecución, y cómo dos de las tres cosas eran la propia puerta
- [`docs/limits.md`](docs/limits.md) - lo que este repositorio no pudo verificar, y por qué
- [`EXAM_MAP.md`](EXAM_MAP.md) - la guía de Databricks Professional, objetivo por objetivo
- [`PARITY.md`](PARITY.md) - la vía de código abierto frente a Databricks, claim a claim
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - `make preflight` es la puerta, y por qué se niega a salir con 0 en una máquina que no puede ejecutar las vías de Spark

Apache-2.0.
