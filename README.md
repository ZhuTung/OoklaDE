# Ookla Speed Connection in BigQuery

- Data sources are in the format of geojson and will convert to csv
- Data will be loaded to Google Cloud Storage in a bucket as data lake
- The data from GCS will be transferred and ingest to Google BigQuery as our data warehouse
- Data will be transformed using Data Build Tool (DBT)
- Power BI will be connected to Google BigQuery for data visualization

![Ookla Diagram](https://github.com/user-attachments/assets/5da95c08-443d-4b15-a9dd-5938521ad957)

### Data Model
![image](https://github.com/user-attachments/assets/9eb944a8-8ee2-4f3b-922f-298f64a6c9b8)
