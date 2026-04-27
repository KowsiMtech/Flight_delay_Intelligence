# Real-Time Flight Delay Intelligence Platform

![Dashboard Screenshot](powerbi/dashboard_screenshot.png)

## North American Aviation Analytics on Databricks

**End-to-End Data Engineering Project**  
Kafka • Databricks • Delta Live Tables • dbt • Unity Catalog • Power BI • GitHub Actions

**Author:** Kowsalya Gopinathan  
**Date:** May 2026

---

## Project Overview
A complete end-to-end real-time data engineering pipeline implementing Streaming Medallion Architecture on Databricks. Ingests 6M+ real BTS flight records simulated as streaming events through Apache Kafka (KRaft mode), processes through Databricks Delta Live Tables, transforms via dbt Core with automated testing, and delivers 5 Power BI dashboards delivering actionable aviation delay intelligence.

## Features
- **Dataset:** BTS On-Time Performance (real US government flight data)
- **Volume:** 6M+ flight records/month (2022-2024)
- **Streaming:** Apache Kafka KRaft mode (no Zookeeper)
- **Processing:** Databricks Auto Loader + Delta Live Tables
- **Transformation:** dbt Core on Databricks SQL Warehouse
- **Governance:** Unity Catalog (full lineage + access control)
- **Quality:** DLT Expectations + dbt tests
- **Dashboards:** 5 Power BI dashboards (DirectQuery & import mode)
- **CI/CD:** GitHub Actions (dbt tests on every push)

## Problem Statement
Flight delays are a major operational and financial challenge for airlines. Each delayed flight cascades into downstream delays, affecting passengers, crew, and aircraft utilization. Airlines and airports require real-time intelligence to proactively manage delay cascades and minimize disruption.

## Business Questions Answered
1. What is each carrier's on-time performance vs industry average?
2. Which routes and airports have highest delay frequency?
3. How does weather correlate with delay spikes?
4. Which aircraft (tail numbers) cause most cascade delays?
5. What are the top delay causes by carrier and season?
6. How has airport congestion trended over time?

## Technology Stack
- **Apache Kafka (KRaft):** Streaming broker (no Zookeeper)
- **Python Kafka Producer:** BTS CSV → Kafka stream simulation
- **ADLS Gen2:** Kafka landing zone + Medallion storage
- **Databricks Auto Loader:** ADLS → Bronze Delta ingestion
- **Delta Live Tables (DLT):** Bronze → Silver transformations
- **dbt Core:** Silver → Gold SQL transformations
- **Unity Catalog:** Data governance + access control
- **Databricks SQL Warehouse:** Gold serving layer
- **Azure Key Vault:** All credentials storage
- **Power BI Desktop:** 5 dashboard pages
- **GitHub Actions:** CI/CD - dbt tests on push
- **OpenMeteo API:** Real-time weather enrichment

## Architecture Overview
![Architecture](architecture/architecture.png)

## Pipeline Flow
1. **Data Sources:** BTS and OpenMeteo (Raw CSV + API)
2. **Producers:** Python Kafka Producer (BTS CSV + Weather API → Kafka topics)
3. **Broker:** Kafka KRaft (Azure VM, 3 topics)
4. **Landing:** ADLS Gen2 (Kafka consumer → Raw JSON files)
5. **Bronze:** Auto Loader (ADLS landing → Delta tables)
6. **Silver:** DLT Pipeline (Bronze Delta → Cleaned + validated Delta)
7. **Gold:** dbt Core (Silver Delta → Business-ready Delta)
8. **Serving:** Databricks SQL Warehouse (Gold Delta → Query endpoint)
9. **BI:** Power BI DirectQuery (SQL Warehouse → 5 dashboards)
10. **CI/CD:** GitHub Actions (Git push → dbt tests + deploy)

## Power BI Dashboards
- **Executive Summary:** High-level KPIs (total flights, on-time %, avg delay, cancellation rate, top carriers)
- **Carrier Performance:** Deep dive into any carrier (on-time trend, route analysis, delay cause breakdown)
- **Airport Operations:** Major airport analytics (departure/arrival delays, congestion, seasonal patterns)
- **Delay Cascade Analysis:** Cascade impact (tail number heatmap, cascade chain visualization)
- **Weather Correlation:** Weather impact (weather vs delay scatter, precipitation impact, wind speed correlation)

## Key Insights
- Processed ~7.3K flights with a 22.02% delay rate; late aircraft is the dominant delay driver.
- SkyWest (OO) underperforms Republic (YX) in delays and cancellations.
- Cascade delays account for 2.09% of total flights, driven by a small subset of aircraft.
- Weather-related delays show limited impact compared to operational factors.

## Cost Analysis (Estimated)
| Service                | Cost (CA$/month) | Notes                                 |
|------------------------|------------------|---------------------------------------|
| Azure VM (Kafka)       | ~30              | Stop when not running                 |
| Databricks Workspace   | ~20-30           | Pause clusters when not in use        |
| ADLS Gen2              | ~2               | Parquet compression, minimal storage  |
| Azure Key Vault        | <1               | Pennies per operation                 |
| GitHub Actions         | 0                | Free for public repos                 |
| Power BI Desktop       | 0                | Free to build and publish             |
| OpenMeteo API          | 0                | Free, no key needed                   |
| **Total**              | **~55-70**       |   |

## Business Recommendations
- Implement tail number monitoring for cascade risk.
- Use weather-triggered operational playbooks.
- Manage peak hour congestion at airports.
- Adopt streaming medallion architecture for real-time intelligence.

## Repository Structure
```
flight-delay-intelligence/
├── .github/
│   └── workflows/
├── kafka/
├── databricks/
├── dbt/
│   └── flight_delay_dbt/
├── powerbi/
│   └── flight_delay_intelligence.pbix
│   └── dashboard_screenshots
├── architecture/
│   └── architecture.png
└── README.md
```

