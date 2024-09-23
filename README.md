# Sleep Data Pipeline with Azure Data Factory

Welcome to the Sleep Data Pipeline project! This advanced system processes and analyzes health-related data, such as sleep tracking, to provide insights and recommendations for users. It utilizes Azure Data Factory and related Azure services to manage the data pipeline.

## Table of Contents
- [Project Overview](#project-overview)
- [Azure Architecture](#azure-architecture)
- [Project Structure](#project-structure)
- [Setup and Configuration](#setup-and-configuration)
- [Usage](#usage)
- [Example: Sleep Data Analysis](#example-sleep-data-analysis)
- [Example: Personalized Sleep Recommendations](#example-personalized-sleep-recommendations)
- [CI/CD with Azure DevOps](#cicd-with-azure-devops)
- [License](#license)

## Project Overview

Our Sleep Data Pipeline is designed to handle large-scale data processing for health and wellness applications. It includes data ingestion from various sources, such as wearable devices and mobile apps, processing, analysis, and visualization components to provide users with personalized insights and recommendations.

Key features:
- Integration with popular health and fitness tracking platforms
- Real-time data ingestion and processing
- Scalable data processing using Azure Data Factory and Azure Databricks
- Machine learning models for sleep pattern analysis and personalized recommendations
- Health trend identification and anomaly detection
- Integration with Azure Cognitive Services for natural language processing of user feedback

## Azure Architecture

Our pipeline utilizes the following Azure services:

- Azure Data Factory: Orchestrates and automates the data movement and transformation
- Azure Blob Storage: Stores raw and processed data
- Azure Databricks: Performs complex data processing and runs machine learning models
- Azure SQL Database: Stores structured data and analysis results
- Azure Analysis Services: Creates semantic models for reporting
- Power BI: Provides interactive dashboards and reports
- Azure Key Vault: Securely stores secrets and access keys
- Azure Monitor: Monitors pipeline performance and health

## Project Structure

```
sleep-azure-pipeline/
│
├── adf/
│   ├── pipeline/
│   │   ├── ingest_sleep_data.json
│   │   ├── process_sleep_patterns.json
│   │   └── generate_recommendations.json
│   ├── dataset/
│   │   ├── sleep_tracker_data.json
│   │   └── processed_sleep_data.json
│   └── linkedService/
│       ├── AzureBlobStorage.json
│       ├── AzureDataLakeStorage.json
│       └── AzureDatabricks.json
│
├── databricks/
│   ├── notebooks/
│   │   ├── sleep_data_analysis.py
│   │   └── sleep_recommendation_model.py
│   └── libraries/
│       └── health_utils.py
│
├── sql/
│   ├── schema/
│   │   ├── sleep_patterns.sql
│   │   └── user_recommendations.sql
│   └── stored_procedures/
│       ├── calculate_sleep_quality.sql
│       └── generate_sleep_recommendations.sql
│
├── power_bi/
│   ├── SleepTrackerDashboard.pbix
│   └── PersonalizedRecommendations.pbix
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── scripts/
│   ├── setup_azure_resources.sh
│   └── deploy_adf_pipelines.sh
│
├── .azure-pipelines/
│   ├── ci-pipeline.yml
│   └── cd-pipeline.yml
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup and Configuration

1. Clone the repository:
   ```
   git clone https://github.com/your-org/health-azure-pipeline.git
   cd health-azure-pipeline
   ```

2. Set up Azure resources:
   ```
   ./scripts/setup_azure_resources.sh
   ```

3. Configure Azure Data Factory pipelines:
   ```
   ./scripts/deploy_adf_pipelines.sh
   ```

4. Set up Azure Databricks workspace and upload notebooks from the `databricks/notebooks/` directory.

5. Create Azure SQL Database schema and stored procedures using scripts in the `sql/` directory.

6. Import Power BI reports from the `power_bi/` directory and configure data sources.

## Usage

1. Monitor and manage Azure Data Factory pipelines through the Azure portal or using Azure Data Factory SDK.

2. Schedule pipeline runs or trigger them manually based on your requirements.

3. Access Databricks notebooks for custom analysis and model training.

4. View reports and dashboards in Power BI for insights into user sleep patterns and personalized recommendations.

## Example: Sleep Data Analysis

In this example, we'll use Azure Databricks to analyze sleep data and identify patterns and trends.

```python
# In Azure Databricks notebook

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, min, max, datediff, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("SleepDataAnalysis").getOrCreate()

# Read sleep data from Azure Data Lake
sleep_data = spark.read.parquet("abfss://processed-data@yourdatalake.dfs.core.windows.net/sleep_data/")

# Convert sleep timestamp to date
sleep_data = sleep_data.withColumn("sleep_date", to_date(col("sleep_start")))

# Calculate sleep statistics by user and date
user_sleep_stats = sleep_data.groupBy("user_id", "sleep_date") \
                    .agg(avg("sleep_duration").alias("avg_sleep_duration"),
                         stddev("sleep_duration").alias("std_dev_sleep_duration"),
                         min("sleep_duration").alias("min_sleep_duration"),
                         max("sleep_duration").alias("max_sleep_duration"))

# Identify users with poor sleep patterns
poor_sleepers = user_sleep_stats.filter((col("avg_sleep_duration") < 6) | (col("std_dev_sleep_duration") > 2)) \
                   .select("user_id", "sleep_date", "avg_sleep_duration", "std_dev_sleep_duration")

# Write results to Azure SQL Database
poor_sleepers.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://yourserver.database.windows.net:1433;database=yourdatabase") \
    .option("dbtable", "poor_sleep_patterns") \
    .option("user", "yourusername") \
    .option("password", "yourpassword") \
    .mode("overwrite") \
    .save()
```

In this example, we:

1. Read sleep data from Azure Data Lake and convert the sleep timestamp to a date column.
2. Calculate sleep statistics (average duration, standard deviation, minimum, and maximum) for each user and date.
3. Identify users with poor sleep patterns, defined as an average sleep duration of less than 6 hours or a standard deviation greater than 2 hours.
4. Write the results of the poor sleeper analysis to an Azure SQL Database table for further use.

These insights can help identify users who may need sleep-related interventions or recommendations.

## Example: Personalized Sleep Recommendations

In this example, we'll use a combination of sleep data analysis and machine learning to generate personalized sleep recommendations.

```python
# In Azure Databricks notebook

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_date
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

# Initialize Spark session
spark = SparkSession.builder.appName("SleepRecommendationModel").getOrCreate()

# Read sleep data and user profile data
sleep_data = spark.read.parquet("abfss://processed-data@yourdatalake.dfs.core.windows.net/sleep_data/")
user_data = spark.read.parquet("abfss://processed-data@yourdatalake.dfs.core.windows.net/user_profiles/")

# Convert sleep and user data timestamps to dates
sleep_data = sleep_data.withColumn("sleep_date", to_date(col("sleep_start")))
user_data = user_data.withColumn("dob", to_date(col("date_of_birth")))

# Calculate user age and other features
user_sleep_data = sleep_data.join(user_data, "user_id") \
                    .withColumn("user_age", datediff(col("sleep_date"), col("dob")) / 365.25)

# Prepare feature vector for machine learning model
featurizer = VectorAssembler(inputCols=["user_age", "sleep_duration", "bedtime", "wake_time"], outputCol="features")
ml_data = featurizer.transform(user_sleep_data)

# Train sleep recommendation model
sleep_model = RandomForestRegressor(labelCol="sleep_quality", featuresCol="features")
sleep_model = sleep_model.fit(ml_data)

# Use the model to generate personalized recommendations
user_recommendations = user_sleep_data.select("user_id", "sleep_date") \
                        .withColumn("recommended_sleep_duration", sleep_model.predict(col("features")))

# Write recommendations to Azure SQL Database
user_recommendations.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://yourserver.database.windows.net:1433;database=yourdatabase") \
    .option("dbtable", "sleep_recommendations") \
    .option("user", "yourusername") \
    .option("password", "yourpassword") \
    .mode("overwrite") \
    .save()
```

In this example, we:

1. Read sleep data and user profile data from Azure Data Lake.
2. Convert the timestamps to date columns for easier manipulation.
3. Calculate additional features, such as user age, based on the data.
4. Prepare a feature vector for the machine learning model, including user age, sleep duration, bedtime, and wake time.
5. Train a RandomForestRegressor model to predict sleep quality based on the input features.
6. Use the trained model to generate personalized sleep duration recommendations for each user and date.
7. Write the recommendations to an Azure SQL Database table for further use, such as displaying them in a user-facing application.

This example demonstrates how to use machine learning to provide personalized sleep recommendations based on a user's sleep patterns and profile data. The recommendations can be used to help users improve their sleep quality and overall health.

## CI/CD with Azure DevOps

We use Azure DevOps for continuous integration and deployment. Our pipeline includes:

1. **Continuous Integration (CI)**
   - Triggered on every push and pull request to the `main` branch
   - Validates Azure Data Factory pipeline definitions
   - Runs unit tests for Databricks notebooks and custom modules
   - Lints SQL scripts and validates database objects

2. **Continuous Deployment (CD)**
   - Triggered on successful merges to the `main` branch
   - Deploys Azure Data Factory pipelines to a staging environment
   - Runs integration tests
   - Upon approval, deploys to the production environment

To view and modify these pipelines, check the `.azure-pipelines/` directory.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
