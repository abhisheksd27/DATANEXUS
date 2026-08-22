terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure Google Cloud Provider
provider "google" {
  project = var.project_id
  region  = var.region
}

# Service Account for DataNexus Pipelines (Airflow, PySpark, dbt)
resource "google_service_account" "pipeline_sa" {
  account_id   = "datanexus-pipeline-sa"
  display_name = "DataNexus Pipeline Service Account"
  description  = "Service account used by Airflow, Spark, and dbt to access GCS & BigQuery"
}

# IAM Role: BigQuery Admin
resource "google_project_iam_member" "sa_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# IAM Role: Storage Admin (GCS)
resource "google_project_iam_member" "sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}