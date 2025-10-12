# ⚡American Renewable Energy Data Lakehouse Platform

## Objective
Contribute to **UN SDG 7 (Clean and Affordable Energy)** through renewable energy data analysis using Data Lakehouse architecture with specialized collection types.

## Overview

**Name**: `renewable-energy-data-lakehouse`  
**Theme**: Energy Sustainability Analysis  
**Alignment**: SDG 7 - Clean and Affordable Energy  

## Public APIs

### 1. EIA Energy Information Administration API - **BATCH**
- **URL**: `https://api.eia.gov/v2/`
- **Collection Type**: **BATCH - Massive Historical Data**
- **Data**: **STRUCTURED** - JSON time series
- **Frequency**: Unlimited BATCH | 1h polling for near real-time
- **Volume**: MASSIVE - Hourly data since 2001 (20+ years)
- **Use**: Historical analysis of American energy matrix
- **Cost**: 100% FREE - No limits

### 2. Open-Meteo Weather API - **STREAM**
- **URL**: `https://api.open-meteo.com/v1/`
- **Collection Type**: **STREAM - Real-time Meteorological Data**
- **Data**: **STRUCTURED** - Meteorological JSON
- **Frequency**: Continuous STREAM or BATCH for historical data (80 years)
- **Volume**: HIGH - Global real-time data
- **Use**: Solar/wind generation forecasting based on climate
- **Cost**: 100% FREE - 10,000 calls/day without API key

### 3. NREL Developer API - **BATCH**
- **URL**: `https://developer.nrel.gov/api/`
- **Collection Type**: **BATCH - Specialized Technical Data**
- **Data**: **UNSTRUCTURED** - Technical PDF reports
- **Frequency**: BATCH 10,000 req/day
- **Volume**: MEDIUM - National specialized technical data
- **Use**: Technical studies and energy efficiency reports
- **Cost**: 100% FREE - 10,000 req/day with registration

### 4. EPA Envirofacts API - **AGGREGATED**
- **URL**: `https://enviro.epa.gov/envirofacts/multisystem.service`
- **Collection Type**: **AGGREGATED - Pre-processed Environmental Data**
- **Data**: **STRUCTURED** - Tabular environmental data
- **Frequency**: AGGREGATED monthly/quarterly
- **Volume**: MEDIUM - Environmental data by region/facility
- **Use**: Correlation emissions vs energy production
- **Cost**: 100% FREE - No limits

### 5. World Bank Open Data API - **AGGREGATED**
- **URL**: `https://api.worldbank.org/v2/`
- **Collection Type**: **AGGREGATED - Development Indicators**
- **Data**: **UNSTRUCTURED** - Policy reports
- **Frequency**: AGGREGATED annual/quinquennial | Monthly polling
- **Volume**: HIGH - 200+ countries, decades of indicators
- **Use**: Global SDG 7 progress benchmarking
- **Cost**: 100% FREE - No limits

## Collection Type Mapping

### 📁 BATCH (Massive Historical Data)
- **EIA API**: Energy time series since 2001
- **NREL API**: Technical reports and studies (PDFs)
- **Open-Meteo**: Historical meteorological data (80 years)

### 📡 STREAM (Real-time via Polling)
- **Open-Meteo API**: Continuously updated meteorological data
- **EIA API**: Hourly polling for "near real-time" data

### 📊 AGGREGATED (Pre-processed Data)
- **EPA Envirofacts**: Environmental indicators by region/period
- **World Bank API**: Development indicators (annual/quinquennial)

## Requirements Compliance

✅ **Multiple sources (4-5)**: 5 specialized public APIs  
✅ **Diversified sources**: Historical energy, real-time climate, technical, environmental, development  
✅ **Structured + unstructured data**: JSON + technical PDFs + reports
✅ **Different types of Data**: String, Float, INT, and more...
✅ **Massive source**: EIA (20+ years) + Open-Meteo (continuous global data)  
✅ **Stream/batch/aggregated**

## Big Data Tools

**Big Data Tools**: Apache Hadoop, HDFS, Apache Spark  
**Integration**: Apache Airflow with specialized workflows  
**Exploration**: Apache Kafka, Apache Flink, Spark Streaming, Delta Lake, Apache Iceberg, Apache Hudi

## Use Cases by Collection Type

### BATCH (Historical Analysis)
1. **American Energy Matrix Evolution (2001-2025)**
2. **Energy Efficiency Analysis by State/Sector**
3. **Historical Correlation: Policies vs Renewable Adoption**

### STREAM (Real-time Analysis)
4. **Solar Generation Forecasting Based on Current Climate**
5. **Wind Energy Conditions Monitoring**
6. **Renewable Generation Opportunity Alerts**

### AGGREGATED (Indicator Analysis)
7. **SDG 7 Progress Benchmarking by Country**
8. **Emissions vs Energy Matrix Correlation by Region**
9. **Global Energy Efficiency Ranking**

## SDG 7 Contribution

**Target 7.1**: Energy access monitoring via World Bank data  
**Target 7.2**: Renewable growth analysis via EIA/NREL data  
**Target 7.3**: Efficiency benchmarking via EPA/World Bank data
