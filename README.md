# Data Engineering - Batch Processing using PySpark

![header](https://github.com/acothaha/img/blob/main/batch_processing_pyspark/Batch%20Processing.png?raw=true)

## **Project Details**

The aim of this project is to drill myself in batch processing with PySpark. This is a rather simple yet comprehensive project

that incorporate multiple tools for the purpose of extract, transform and load data then finally visualizing it.


## **Project Datasets**

In this project, I ,for the most part, am handling 3 open datasets that i retrieved from a NYC Taxi & Limousine Commission ([site](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)):

- Yellow Taxi Trip Records:

    This data includes fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts of yellow taxi in NYC

- Green Taxi Trip Records

    This data includes fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts of green taxi in NYC

- Taxi Zone Lookup

    This data includes the information of the name of zones according to their code

## **Tools and Technologies**

- Python (Pandas, GCP SDK, Prefect SDK)
- PySpark
- Docker
- GCP services (BigQuery, Looker)
- Prefect 
- ETL: Extract, Transform, Load Data
- Data Warehouse Concepts
- Data Modeling
- Cloud Computing Concepts
- Bash

## **Project Steps**

![flow](https://github.com/acothaha/img/blob/main/batch_processing_pyspark/flow.png?raw=true)

### **Step 1: Define scope of the project and necessary resources**

- Identify and search for data needed for the project. Determine the end use cases to prepare the data for (e.g., Analytics table, machine learning, etc.)
- Formulate the data lifecycle.
- Initiate a cloud architecture (GCP, prefect) to be used for the project.
- Commence an environment needed for the project (Docker) (Not yet)

### **Step 2: Gather data**
- Create a python script to collect sample data from the Internet
- Save it into local machine

### **Step 3: Explore and assess the data**
- Explore the data to identify data quality issues, like missing values, duplicate data, etc.
- Create data cleaning and wrangling procedure

### **Step 4: Define the data model**
- Mapping out the conceptual data model and explain why this model is used.
- planning to pipeline the data into the data model.

### **Step 5: Run ETL to model the data**
- Create the data pipelines and the data model
- Include a version control
- utilize orchestration as an attemp of monitoring the workflow
- Automate the process

### **Step 6: Visualization**
- Create a visualization according to the use case
- Deploy the visualization







