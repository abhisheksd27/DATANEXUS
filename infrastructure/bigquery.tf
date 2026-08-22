# 1. Raw Dataset (Loads external Parquet files directly from Gold GCS zone)
resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id                  = "datanexus_raw"
  friendly_name               = "DataNexus Raw Ingestion Schema"
  description                 = "Raw data loaded directly from Gold zone GCS Parquet files"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# 2. Staging Dataset (dbt intermediate transformations)
resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id                  = "datanexus_staging"
  friendly_name               = "DataNexus dbt Staging Schema"
  description                 = "Cleaned and standardized intermediate dbt models"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# 3. Analytics Dataset (Final Star Schema: Fact & Dimension tables)
resource "google_bigquery_dataset" "analytics_dataset" {
  dataset_id                  = "datanexus_analytics"
  friendly_name               = "DataNexus Analytics Production Schema"
  description                 = "Final business-ready Fact and Dimension models for dashboards"
  location                    = var.region
  delete_contents_on_destroy  = true
}
