# omop-constructs

**omop-constructs** is a small library for building **reusable, composable analytical constructs on top of OMOP CDM** using SQLAlchemy.

It sits “above” low-level OMOP table models (from **omop-alchemy**) and semantic definitions (from **omop-semantics**), and provides a way to package up **common query patterns, mappings, and derived views** into reusable units.

In practice, this means you can define things like:

- tumour staging logic (T/N/M, group stage),
- clinical modifiers (grade, laterality, size),
- condition + modifier joins,
- episode-linked phenotypes,
- reusable materialized views or query fragments,

and then reuse them consistently across:

- analytics code,
- cohort definitions,
- ETL / feature engineering,
- dashboards and reports.

---

## What problem does this solve?

When working with OMOP in real research settings, you often end up re-implementing the same patterns:

- joining measurements or observations back to conditions,  
- resolving multiple staging systems (clinical vs pathological),  
- preferring “best available” records (earliest, latest, ranked),  
- materialising complex derived tables for performance,  
- wiring together multiple OMOP tables into analysis-ready shapes.

These patterns are:

- **non-trivial SQL**,  
- **project-specific but reusable**,  
- and easy to drift or fork across notebooks, pipelines, and services.

**omop-constructs gives you a place to define these patterns once**, as explicit, testable Python objects built on SQLAlchemy, and then reuse them anywhere you use OMOP.

Think of it as a library of **semantic query building blocks** for OMOP.

---

## Relationship to other libraries

- **omop-alchemy**  
  Provides the canonical, typed SQLAlchemy models for OMOP CDM tables.

- **omop-semantics**  
  Defines which concepts, groups, and roles mean what in your domain (e.g. staging, modifiers).

- **omop-constructs**  
  Uses both of the above to build **higher-level analytical constructs**, such as:

  - derived views,
  - reusable joins,
  - staged phenotype tables,
  - canonical query fragments.

In short:

> omop-alchemy defines *the schema*  
> omop-semantics defines *the meaning*  
> omop-constructs defines *the reusable analytical shapes*

---

## Core ideas

- **Composable query building blocks**  
  Encapsulate common SQL patterns as reusable Python objects.

- **SQLAlchemy-first**  
  Constructs are normal SQLAlchemy `select()` expressions, subqueries, and ORM-mapped views.

- **Reusable analytical units**  
  Package complex mappings (e.g. staging logic) once and reuse them across contexts.

- **Materialized view support**  
  Support for defining derived tables and materialized views for performance and stability.

- **Semantics-aware**  
  Integrates with omop-semantics concept registries and lookups, rather than hard-coding concept IDs.

---

## Example use case (high level)

A typical construct might:

- take OMOP `Measurement` rows representing staging concepts,  
- classify them into T, N, M, and group stage using semantic lookups,  
- rank multiple records per condition to select the “best” stage,  
- expose the result as a materialized view that can be joined back to <code>Condition_Occurrence</code>.

This allows downstream code to work with a clean, analysis-ready table like:

> “conditions with resolved TNM stage and modifiers”

without re-implementing the logic every time.


---

## Typical workflow

1. **Define semantic lookups**  
   Use <code>omop-semantics</code> to define which concepts represent staging, grading, laterality, etc.

2. **Build constructs**  
   Use SQLAlchemy and <code>omop-alchemy</code> models to define reusable queries and derived views.

3. **Materialize or compose**  
   Optionally materialize complex constructs into views or tables for performance.

4. **Reuse everywhere**  
   Import the same construct into analytics notebooks, ETL jobs, or services.

---

## When should you use this?

Use **omop-constructs** if you:

- repeatedly write and share complex OMOP joins and mappings,  
- need consistent, reusable phenotype or feature definitions,  
- want complex logic to live in one place that is versionable and extensible,  
- are building analytics pipelines or research platforms on OMOP,  
- care about making your analytical layer explicit and testable.


---

## Design goals

- Declarative, explicit constructs  
- SQLAlchemy-native  
- No hidden execution or side effects  
- Easy to test in isolation  
- Compatible with materialized views and derived tables  
- Portable across PostgreSQL, SQLite, and other SQLAlchemy backends