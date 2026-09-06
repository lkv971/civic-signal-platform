# Civic Signal Platform

Civic Signal is a public, synthetic reference implementation of a modern procurement-intelligence platform built with Microsoft Fabric, Power BI, governed semantic modeling, and AI-assisted analytics.

The project demonstrates how enterprise data-engineering, analytics, governance, and AI patterns can be implemented end to end without exposing production systems, customer data, proprietary scoring logic, or commercial source integrations.

> **Portfolio scope:** All procurement data in this repository is synthetic and exists only for technical demonstration.

---

## What This Project Demonstrates

Civic Signal is designed as a verifiable technical portfolio rather than a mock architecture diagram.

The repository contains source-controlled Microsoft Fabric artifacts that demonstrate:

- medallion-style Bronze, Silver, and Gold data processing;
- Fabric Lakehouse and Warehouse patterns;
- notebook-based ingestion and transformation;
- data-pipeline orchestration;
- governed dimensional and analytical modeling;
- Power BI semantic modeling;
- explicit business measures and relationships;
- Power BI report development;
- Prep data for AI;
- curated AI-facing schemas;
- AI instructions;
- Verified Answers;
- Microsoft Fabric Data Agent;
- Git-integrated Fabric application lifecycle management.

---

## Current Architecture

```text
Synthetic procurement sources
          |
          v
+----------------------+
| Bronze               |
| Raw synthetic data   |
+----------------------+
          |
          v
+----------------------+
| Silver               |
| Cleaned and governed |
| analytical entities  |
+----------------------+
          |
          v
+----------------------+
| Gold                 |
| Curated analytical   |
| warehouse structures |
+----------------------+
          |
          v
+-------------------------------+
| SM_CivicSignal_Intelligence   |
|                               |
| - Governed dimensions         |
| - Explicit measures           |
| - Relationships               |
| - Business descriptions       |
+-------------------------------+
          |
          +----------------------+
          |                      |
          v                      v
+--------------------+   +---------------------------+
| Power BI Report    |   | Prep data for AI          |
|                    |   |                           |
| Civic intelligence |   | - Simplified AI schema    |
| analytics          |   | - AI instructions         |
+--------------------+   | - Verified Answers        |
                         +---------------------------+
                                      |
                                      v
                         +---------------------------+
                         | DA_CivicSignal_            |
                         | Intelligence               |
                         |                           |
                         | Fabric Data Agent         |
                         +---------------------------+
```

---

## Microsoft Fabric Implementation

The `fabric/` directory contains the source-controlled Fabric implementation.

### Data Platform

Key artifacts include:

- `LH_CivicSignal_Bronze`
- `LH_CivicSignal_Silver`
- `WH_CivicSignal_Gold`
- synthetic Bronze ingestion notebooks
- Silver transformation notebooks
- Fabric Data Pipelines for orchestration

### Analytics

- `SM_CivicSignal_Intelligence`
- `RP_CivicSignal_Intelligence`

### AI

- `DA_CivicSignal_Intelligence`

The Data Agent is grounded only in the governed semantic model rather than directly querying raw analytical storage. This keeps business semantics centralized in one governed layer.

---

## Semantic Model

`SM_CivicSignal_Intelligence` provides the analytical contract for the reporting and AI layers.

The model includes domains such as:

- Date
- Source
- Buyer
- Procurement Category
- Opportunities
- Duplicate Candidates

The semantic model uses:

- governed business-friendly names;
- explicit measures;
- defined relationships;
- curated descriptions;
- hidden technical fields where appropriate;
- a deliberately simplified schema for AI consumption.

---

## Power BI

`RP_CivicSignal_Intelligence` demonstrates procurement-intelligence reporting built over the governed semantic model.

The report is designed to show analytical patterns such as:

- procurement opportunity monitoring;
- buyer intelligence;
- source analysis;
- procurement-category analysis;
- duplicate-candidate review.

A hidden support page is also used to configure trusted Verified Answers for AI scenarios.

---

## Fabric AI Data Agent

`DA_CivicSignal_Intelligence` demonstrates semantic-model-grounded natural-language analytics using Microsoft Fabric Data Agent.

The implementation includes:

- curated AI schema selection;
- semantic-model AI instructions;
- Verified Answers;
- agent-level instructions;
- governed business terminology;
- explicit handling of missing or unclassified values;
- protection against fabrication of missing procurement facts;
- Standard runtime configuration;
- published Data Agent configuration;
- Git-integrated Fabric ALM.

See [`docs/fabric-ai-data-agent.md`](docs/fabric-ai-data-agent.md) for the detailed AI architecture.

---

## Governance Principles

The implementation follows several core principles:

1. **Govern business meaning upstream.** Business definitions belong in the analytical and semantic layers rather than being recreated independently in reports or AI agents.

2. **Prefer explicit measures.** AI and reporting should use governed semantic-model measures instead of ambiguous implicit aggregation.

3. **Keep technical fields away from business users.** Keys, processing metadata, and engineering fields are hidden or excluded from the main analytical experience where appropriate.

4. **Do not fabricate missing information.** Missing buyers, categories, values, dates, and other procurement facts remain explicitly unresolved.

5. **Treat analytical indicators as signals.** Duplicate-candidate and data-quality indicators support review; they are not presented as official procurement determinations.

6. **Separate portfolio data from production systems.** Civic Signal uses synthetic data and intentionally contains no customer data, credentials, proprietary scoring logic, or commercial implementation details.

---

## Repository Structure

```text
civic-signal-platform/
|
|-- fabric/
|   |-- Lakehouses
|   |-- Warehouse
|   |-- Notebooks
|   |-- Data Pipelines
|   |-- Semantic Model
|   |-- Power BI Report
|   `-- Fabric Data Agent
|
|-- docs/
|   `-- fabric-ai-data-agent.md
|
|-- .codex/
|-- .vscode/
`-- README.md
```

---

## Application Lifecycle Management

The Fabric implementation is integrated with Git.

Development follows a branch-based workflow:

```text
fabric-dev
    |
    v
Pull Request
    |
    v
main
```

Fabric-generated definitions and supporting documentation are version controlled so the implementation can be reviewed as code rather than demonstrated only through screenshots.

---

## Roadmap

Planned extensions include:

- Azure Databricks reference implementation;
- Unity Catalog governance;
- Databricks Bronze / Silver / Gold processing;
- Delta Lake engineering patterns;
- Lakeflow Jobs and orchestration;
- Fabric ↔ Databricks interoperability;
- CI/CD and deployment automation;
- additional AI-agent patterns.

The Databricks implementation will remain synthetic and sanitized, following the same public-portfolio boundary as the Fabric implementation.

---

## Technology Stack

### Current

- Microsoft Fabric
- Fabric Lakehouse
- Fabric Warehouse
- Fabric Notebooks
- Fabric Data Pipelines
- Power BI
- Power BI semantic models
- Prep data for AI
- Verified Answers
- Microsoft Fabric Data Agent
- Git / GitHub

### Planned

- Azure Databricks
- Unity Catalog
- Delta Lake
- Lakeflow Jobs
- Databricks Asset Bundles

---

## Disclaimer

Civic Signal is a synthetic technical reference implementation.

It is not a government procurement service, production tender portal, commercial procurement platform, or source of official procurement information.

All example procurement records are synthetic and exist solely to demonstrate engineering, analytics, governance, and AI capabilities.
