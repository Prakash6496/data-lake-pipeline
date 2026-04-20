# 🪣 AWS S3 Data Lake Simulation

A Python pipeline that simulates a real AWS S3 data lake
using MinIO — uploads weather data in date-partitioned
folders exactly like production data engineering teams!

## 🚀 Features
- Local S3-compatible storage with MinIO
- Date-partitioned file organization
- Uploads raw JSON, processed CSV, and summary JSON
- Reads back and verifies data from lake
- Standard data lake architecture (raw/processed/summary layers)

## 🛠️ Tech Stack
- Python 3.x
- boto3 — AWS S3 SDK
- MinIO — local S3-compatible storage
- pandas — data processing
- requests — API calls

## ▶️ How to Run

### 1. Start MinIO server
.\minio.exe server C:\minio-data --console-address ":9001"

### 2. Install dependencies
pip install boto3 pandas requests

### 3. Run pipeline
python data_lake_pipeline.py

### 4. View data lake UI
http://localhost:9001
Username: minioadmin
Password: minioadmin

## 📁 Data Lake Structure
weather-data-lake/
├── raw/
│   └── year=2026/month=04/day=18/hour=21/
│       └── weather_raw.json
├── processed/
│   └── year=2026/month=04/day=18/hour=21/
│       └── weather_processed.csv
└── summary/
    └── year=2026/month=04/day=18/hour=21/
        └── weather_summary.json

## 🔗 Data Source
Open-Meteo — Free weather API