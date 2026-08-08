# FinGuard 🛡️

> Real-time fraud detection data pipeline built on Databricks

## 📋 Overview

FinGuard is a comprehensive real-time fraud detection and financial monitoring system built on the Databricks Lakehouse platform. It processes streaming financial transactions, identifies fraudulent patterns, and sends instant alerts to prevent financial losses.

## 🏗️ Architecture

### High-Level System Design

```
╔═══════════════════════════════════════════════════════════════════════╗
║                          FINGUARD ARCHITECTURE                        ║
║                    Real-Time Fraud Detection Pipeline                 ║
╚═══════════════════════════════════════════════════════════════════════╝

┌───────────────────────────────────────────────────────────────────────┐
│                          📥 INGESTION LAYER                           │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   │  🌊 Kafka       │      │  📁 Auto Loader │      │  🗂️  Watchlist  │
│   │  Streaming      │      │  File Upload    │      │  External Data  │
│   │                 │      │                 │      │                 │
│   │  • Real-time    │      │  • CSV/JSON     │      │  • Known Fraud  │
│   │  • High volume  │      │  • Schema Auto  │      │  • Blacklists   │
│   │  • Low latency  │      │  • Incremental  │      │  • Risk Scores  │
│   └────────┬────────┘      └────────┬────────┘      └────────┬────────┘
│            │                        │                        │       │
└────────────┼────────────────────────┼────────────────────────┼───────┘
             │                        │                        │
             └────────────────────────┼────────────────────────┘
                                      ▼
┌───────────────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER - Raw Data Lake                    │
├───────────────────────────────────────────────────────────────────────┤
│  Technology: Delta Lake | Format: Parquet | Compression: Snappy      │
│                                                                       │
│   ┌─────────────────────────────┐      ┌──────────────────────────────┐
│   │  transactions_bronze        │      │  fraud_watchlist_bronze      │
│   ├─────────────────────────────┤      ├──────────────────────────────┤
│   │  • Raw transaction data     │      │  • Fraud entity lists        │
│   │  • No transformations       │      │  • Risk classifications      │
│   │  • Full history retained    │      │  • Regular updates           │
│   │  • Schema validation        │      │  • Versioned snapshots       │
│   └──────────────┬──────────────┘      └──────────────┬───────────────┘
│                  │                                     │              │
└──────────────────┼─────────────────────────────────────┼──────────────┘
                   │                                     │
                   └──────────────┬──────────────────────┘
                                  ▼
┌───────────────────────────────────────────────────────────────────────┐
│                🥈 SILVER LAYER - Cleansed & Enriched                  │
├───────────────────────────────────────────────────────────────────────┤
│  Data Quality: Validated | Deduplicated | Type-Safe | Business Rules │
│                                                                       │
│   ┌─────────────────────────────┐      ┌──────────────────────────────┐
│   │  customers_silver           │      │  enriched_transactions       │
│   ├─────────────────────────────┤      ├──────────────────────────────┤
│   │  • Customer master data     │◄─────┤  • Joined with customer data │
│   │  • Validated records        │ Join │  • Fraud score calculated    │
│   │  • Active accounts only     │      │  • Enriched with metadata    │
│   │  • SCD Type 2 history       │      │  • Filtered & cleaned        │
│   └─────────────────────────────┘      └──────────────┬───────────────┘
│                                                        │              │
└────────────────────────────────────────────────────────┼──────────────┘
                                                         ▼
┌───────────────────────────────────────────────────────────────────────┐
│                      🎯 FRAUD DETECTION ENGINE                        │
├───────────────────────────────────────────────────────────────────────┤
│  Processing: Real-Time | ML-Ready | Rule-Based | Pattern Recognition │
│                                                                       │
│   ╔═════════════════════════════════════════════════════════════╗   │
│   ║                      Detection Algorithms                   ║   │
│   ╠═════════════════════════════════════════════════════════════╣   │
│   ║                                                             ║   │
│   ║  🔍 Watchlist Matching                                      ║   │
│   ║     └─ Cross-reference with known fraud entities           ║   │
│   ║     └─ Card/Merchant/Customer blacklist checks             ║   │
│   ║                                                             ║   │
│   ║  💰 High-Value Transaction Alerts                          ║   │
│   ║     └─ Configurable threshold monitoring                   ║   │
│   ║     └─ Currency-aware amount checks                        ║   │
│   ║                                                             ║   │
│   ║  📊 Anomaly Detection (ML-Ready)                           ║   │
│   ║     └─ Behavioral pattern analysis                         ║   │
│   ║     └─ Velocity checks (transaction frequency)             ║   │
│   ║     └─ Geographic anomalies                                ║   │
│   ║                                                             ║   │
│   ║  ⚡ Real-Time Scoring                                       ║   │
│   ║     └─ Composite risk score calculation                    ║   │
│   ║     └─ Confidence level assignment                         ║   │
│   ║                                                             ║   │
│   ╚═════════════════════════════════════════════════════════════╝   │
│                                  │                                    │
└──────────────────────────────────┼────────────────────────────────────┘
                                   ▼
┌───────────────────────────────────────────────────────────────────────┐
│                    📢 ALERT & NOTIFICATION SYSTEM                     │
├───────────────────────────────────────────────────────────────────────┤
│  Delivery: Multi-Channel | Priority-Based | Configurable Rules       │
│                                                                       │
│   ┌──────────────────────────┐           ┌──────────────────────────┐
│   │  📧 Email Alerts         │           │  📊 Dashboard Updates    │
│   ├──────────────────────────┤           ├──────────────────────────┤
│   │  • SMTP Integration      │           │  • Lakeview Dashboards   │
│   │  • Rich HTML templates   │           │  • Real-time metrics     │
│   │  • Priority tagging      │           │  • Historical trends     │
│   │  • Fraud analyst team    │           │  • Drill-down analysis   │
│   └──────────────────────────┘           └──────────────────────────┘
│                                                                       │
│   ┌──────────────────────────┐           ┌──────────────────────────┐
│   │  📝 Audit Logging        │           │  🔔 Future: Slack/Teams  │
│   ├──────────────────────────┤           ├──────────────────────────┤
│   │  • Complete trail        │           │  • Instant notifications │
│   │  • Compliance reporting  │           │  • Bot integration       │
│   │  • Investigation support │           │  • Channel routing       │
│   └──────────────────────────┘           └──────────────────────────┘
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘

╔═══════════════════════════════════════════════════════════════════════╗
║                          TECHNOLOGY STACK                             ║
╠═══════════════════════════════════════════════════════════════════════╣
║  Platform      : Databricks Lakehouse (Unity Catalog)                ║
║  Storage       : Delta Lake (ACID transactions, time travel)         ║
║  Streaming     : Apache Kafka + Structured Streaming                 ║
║  Processing    : Apache Spark (PySpark)                              ║
║  Orchestration : Delta Live Tables (DLT)                             ║
║  Languages     : Python, SQL                                         ║
║  Monitoring    : Lakeview Dashboards                                 ║
╚═══════════════════════════════════════════════════════════════════════╝
```

### Data Flow Summary

1. **Ingestion** → Raw data from multiple sources (Kafka streams, file uploads, external APIs)
2. **Bronze Layer** → Immutable raw data storage with full history
3. **Silver Layer** → Validated, cleansed, and enriched data ready for analysis
4. **Detection Engine** → Real-time fraud analysis using multiple algorithms
5. **Alerting** → Multi-channel notifications to fraud analysts and stakeholders


## ✨ Key Features

### 🔄 Real-Time Streaming
- **Kafka Integration**: Ingests transaction data from Kafka streams
- **Auto Loader**: Handles file-based data ingestion with schema evolution
- **Delta Live Tables**: Ensures data quality and lineage

### 🎯 Fraud Detection
- **Watchlist Matching**: Cross-references transactions against known fraud patterns
- **High-Value Alerts**: Flags transactions exceeding configurable thresholds
- **Pattern Recognition**: ML-ready architecture for advanced anomaly detection

### 📧 Alerting System
- **Instant Email Notifications**: Sends alerts to fraud analysts in real-time
- **Configurable Rules**: Customizable alert thresholds and conditions
- **Multi-Channel Support**: Email, dashboard, and extensible to Slack/PagerDuty

### 🏛️ Medallion Architecture
- **Bronze Layer**: Raw data ingestion with minimal transformation
- **Silver Layer**: Cleansed, validated, and enriched data
- **Gold Layer**: Business-ready aggregations and analytics (extensible)

## 📁 Project Structure

```
finguard/
├── 01_kafka_streaming_test.ipynb          # Kafka stream ingestion
├── 02_Setup_Secret_Source.ipynb           # Credentials & secrets setup
├── 03_sql_test.ipynb                      # SQL queries & validation
├── 04_Send_Email.ipynb                    # Email notification logic
├── 05_Autoloader_test.ipynb               # File ingestion via Auto Loader
│
├── finguard_streaming/
│   ├── bronze/
│   │   ├── transactions_bronze.py         # Raw transaction ingestion
│   │   └── fraud_watchlist_bronze.py      # Fraud watchlist ingestion
│   │
│   ├── silver/
│   │   └── transactions_silver.py         # Cleansed transactions
│   │
│   └── alert/
│       ├── fraud_card_alert_email_notifier.ipynb  # Fraud alerts
│       └── high_value_transaction_email_notifier.py  # High-value alerts
│
├── finguard_customers_silver_load_2/
│   └── silver/
│       └── customers_silver.py            # Customer dimension table
│
└── fraud_watchlist_file_generator/
    └── (watchlist generation utilities)
```

## 🚀 Getting Started

### Prerequisites
- Databricks workspace (AWS/Azure/GCP)
- Unity Catalog enabled
- Kafka cluster (for streaming ingestion)
- SMTP server for email alerts

### Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/GauravSethi22/finguard.git
   ```

2. **Configure Secrets** (Run `02_Setup_Secret_Source.ipynb`)
   ```python
   # Store credentials in Databricks Secrets
   # - Kafka credentials
   # - SMTP credentials
   # - API keys
   ```

3. **Set Up Data Sources**
   - Configure Kafka topics for transaction streams
   - Set up file upload locations for Auto Loader
   - Load initial fraud watchlist

4. **Deploy Streaming Pipelines**
   - Run bronze layer notebooks to ingest raw data
   - Execute silver layer transformations
   - Configure alert thresholds

5. **Test the Pipeline**
   ```bash
   # Run test notebooks in order:
   # 1. 01_kafka_streaming_test.ipynb
   # 2. 03_sql_test.ipynb
   # 3. 04_Send_Email.ipynb
   ```

## 🔧 Configuration

### Environment Variables
```python
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "your-kafka-broker:9092"
KAFKA_TOPIC = "financial-transactions"

# Alert Thresholds
HIGH_VALUE_THRESHOLD = 10000  # USD
FRAUD_SCORE_THRESHOLD = 0.75

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
ALERT_RECIPIENTS = ["fraud-team@company.com"]
```

## 📊 Data Schema

### Transaction Schema
```json
{
  "transaction_id": "string",
  "customer_id": "string",
  "card_number": "string (masked)",
  "amount": "decimal",
  "currency": "string",
  "merchant_name": "string",
  "merchant_category": "string",
  "transaction_timestamp": "timestamp",
  "location": "struct<latitude:double, longitude:double>",
  "fraud_score": "double"
}
```

### Fraud Watchlist Schema
```json
{
  "entity_id": "string",
  "entity_type": "string",  // "card", "merchant", "customer"
  "risk_level": "string",    // "high", "medium", "low"
  "reason": "string",
  "added_date": "timestamp"
}
```

## 📈 Monitoring & Observability

- **Real-Time Dashboard**: FinGuard Lakeview dashboard for live monitoring
- **Data Quality Checks**: Built-in validation at each medallion layer
- **Alert Logs**: Comprehensive audit trail of all fraud alerts
- **Performance Metrics**: Streaming lag, throughput, and latency tracking

## 🔐 Security

- **Data Encryption**: End-to-end encryption for sensitive data
- **Access Controls**: Unity Catalog integration for fine-grained permissions
- **Secret Management**: Databricks Secrets for credential storage
- **Audit Logging**: Complete lineage and access tracking

## 🛠️ Tech Stack

- **Platform**: Databricks Lakehouse
- **Storage**: Delta Lake
- **Streaming**: Apache Kafka, Databricks Structured Streaming
- **Orchestration**: Delta Live Tables
- **Visualization**: Databricks Lakeview Dashboards
- **Language**: Python, SQL

## 📝 Future Enhancements

- [ ] ML-based anomaly detection using Databricks ML
- [ ] Integration with external fraud scoring APIs
- [ ] Advanced behavioral analytics
- [ ] Real-time dashboard with predictive insights
- [ ] Multi-region deployment support
- [ ] Slack/Teams integration for alerts

## 🤝 Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👤 Author

**Gaurav Sethi**
- GitHub: [@GauravSethi22](https://github.com/GauravSethi22)

## 🙏 Acknowledgments

- Built on Databricks Lakehouse Platform
- Inspired by real-world fraud detection challenges
- Community contributions and feedback

---

⭐ If you find this project helpful, please consider giving it a star!

**Note**: This is a demonstration project. For production use, ensure proper security reviews, compliance checks, and performance testing.