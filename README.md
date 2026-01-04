# ⚡American Renewable Energy Data Lakehouse Platform

## Objective
Contribute to **UN SDG 7 (Clean and Affordable Energy)** through renewable energy data analysis using Data Lakehouse architecture with specialized collection types.

## Overview

**Name**: `renewable-energy-lakehouse`  
**Theme**: Energy Sustainability Analysis  
**Alignment**: SDG 7 - Clean and Affordable Energy  

### Technnologies
- **Apache Airflow**: `3.1.5`
- **Python**: `3.12.x`
- **pandas**: `2.2.0` or higher
- **pyarrow**: `14.0.0` or higher
- **Operacional**: Linux/macOS (Windows: use WSL2)

⚠️ **IMPORTANT**: Este projeto **NÃO funciona** com Airflow 2.x. Se você tem Airflow 2.x instalado, precisará migrar.

## Project Structure

```
renewable-energy-pipeline/
├── .gitignore
├── README.md
├── docker-compose.yml
├── .env                                    # Variáveis de ambiente e API keys
│
├── dags/
│   ├── __init__.py
│   ├── dag_stream_openmeteo_weather.py     # STREAM - Open Meteo
│   ├── dag_emission_intensity.py           # BATCH + AGGREGATED - EIA + EPA intensidade de emissões
│   └── dag_solar_capacity_factor.py        # BATCH + AGGREGATED - EIA + NREL + OpenMeteo fator capacidade solar
│
├── plugins/
│   ├── __init__.py
│   │
│   ├── hooks/
│   │   ├── __init__.py
│   │   ├── eia_hook.py                     # Hook para EIA API
│   │   ├── epa_hook.py                     # Hook para EPA API
│   │   ├── nrel_hook.py                    # Hook para NREL API
│   │   └── openmeteo_hook.py               # Hook para Open-Meteo API
│   │
│   └── custom_operators/
│   │   ├── __init__.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── transformations.py      
│       ├── state_utils.py
│       ├── data_helpers.py              
│       └── solar_transformations.py 
│
├── config/
│   ├── airflow.cfg                         # Configuração customizada do Airflow
│
├── logs/                                   # Gerado automaticamente pelo Airflow
│   └── .gitkeep
│
├── data/                                   # Volume montado (bronze/silver/gold)
│   ├── raw/                                # BRONZE - Dados brutos
│   │   ├── eia/
│   │   ├── epa/
│   │   ├── nrel/
│   │   └── openmeteo/
│   │
│   └── processed/                          # SILVER - Dados processados/transformados
│
├── variables/                              # Airflow Variables JSON
│   └── api_keys.json
```

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

## Big Data Tools

**Integration**: Apache Airflow with specialized workflows

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