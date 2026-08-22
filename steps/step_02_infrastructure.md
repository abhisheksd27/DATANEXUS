# Step 2 — Cloud Infrastructure as Code (GCP & Terraform Setup)

## What We Are Doing
Provisioning all necessary cloud resources on **Google Cloud Platform (GCP)** using **Terraform (Infrastructure as Code - IaC)**.

Instead of manually clicking around in the GCP Web Console, we write declarative configuration files (`.tf`) that define our cloud storage, warehouse datasets, service accounts, and IAM roles.

---

## Why Terraform & IaC?
- **Reproducibility:** You can spin up or tear down the entire cloud environment across dev, staging, and prod with identical configurations.
- **Version Control:** All infrastructure changes are tracked in Git.
- **State Management:** Terraform keeps a state file (`terraform.tfstate`) to map your local configuration to real cloud resources.
- **Safety Preview (`plan`):** You see exact additions, modifications, and deletions before applying any change.

---

## Cloud Resources Being Provisioned

| Resource Type | Resource Name | Purpose |
|---|---|---|
| **Google Cloud Storage (GCS)** | `datanexus-datalake-<project_id>` | Medallion Data Lake bucket containing `bronze/`, `silver/`, `gold/` folders |
| **BigQuery Dataset** | `datanexus_raw` | Schema for raw ingested external tables / direct loads |
| **BigQuery Dataset** | `datanexus_staging` | Schema for intermediate cleaned models managed by dbt |
| **BigQuery Dataset** | `datanexus_analytics` | Production schema for Fact and Dimension tables (Star Schema) |
| **Service Account** | `datanexus-pipeline-sa` | Dedicated robot identity used by Airflow, PySpark, and dbt |
| **IAM Policy Bindings** | `roles/storage.admin`, `roles/bigquery.admin` | Access permissions for pipeline operations |

---

## Technologies in Step 2

### 1. Terraform
- Declarative infrastructure as code tool created by HashiCorp.
- Uses **HashiCorp Configuration Language (HCL)**.
- Lifecycle workflow: `terraform init` ➔ `terraform plan` ➔ `terraform apply` ➔ `terraform destroy`.

### 2. Google Cloud Storage (GCS)
- Google's scalable object storage service.
- Functions as our **Data Lake**.
- Configured with:
  - `uniform_bucket_level_access = true` for unified IAM access control.
  - `lifecycle_rule` to transition data older than 90 days to cheaper Nearline storage.
  - `versioning` enabled for backup and disaster recovery.

### 3. Google BigQuery
- Serverless, highly-scalable, columnar data warehouse.
- Used as our **Data Warehouse (OLAP)**.
- In Step 6, **dbt** will transform tables across the 3 datasets provisioned here.

### 4. GCP IAM (Identity & Access Management) & Service Accounts
- Service Accounts act as application identities (non-human accounts).
- Pipelines (Spark jobs, Airflow DAGs, dbt models) authenticate using service account credentials.

---

## Folder Structure for Step 2

```
DataNexus/
└── infrastructure/
    ├── variables.tf     <-- Project ID, region, and bucket variables
    ├── main.tf          <-- Provider configuration & Service Account setup
    ├── gcs.tf           <-- GCS Data Lake bucket & folder placeholders
    ├── bigquery.tf      <-- Raw, Staging, and Analytics BigQuery schemas
    └── outputs.tf       <-- Exported values (bucket name, dataset IDs, SA email)
```

---

## Complete Code Files

### 1. `infrastructure/variables.tf`
```hcl
variable "project_id" {
  description = "The Google Cloud Platform (GCP) Project ID"
  type        = string
  default     = "your-gcp-project-id" # Replace with your real GCP Project ID
}

variable "region" {
  description = "Primary GCP region for regional resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "gcs_bucket_name" {
  description = "Globally unique name for the GCS Data Lake storage bucket"
  type        = string
  default     = "datanexus-datalake-dev-storage"
}
```

### 2. `infrastructure/main.tf`
```hcl
terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

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
```

### 3. `infrastructure/gcs.tf`
```hcl
# GCS Bucket for Data Lake (Medallion Architecture Storage)
resource "google_storage_bucket" "datalake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  versioning {
    enabled = true
  }
}

# Medallion Zone Folder Objects
resource "google_storage_bucket_object" "bronze_folder" {
  name    = "bronze/"
  content = "DataNexus Bronze Raw Zone"
  bucket  = google_storage_bucket.datalake.name
}

resource "google_storage_bucket_object" "silver_folder" {
  name    = "silver/"
  content = "DataNexus Silver Cleaned Zone"
  bucket  = google_storage_bucket.datalake.name
}

resource "google_storage_bucket_object" "gold_folder" {
  name    = "gold/"
  content = "DataNexus Gold Aggregated Zone"
  bucket  = google_storage_bucket.datalake.name
}
```

### 4. `infrastructure/bigquery.tf`
```hcl
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
```

### 5. `infrastructure/outputs.tf`
```hcl
output "gcs_bucket_name" {
  description = "The created GCS Data Lake bucket name"
  value       = google_storage_bucket.datalake.name
}

output "bigquery_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw_dataset.dataset_id
}

output "bigquery_staging_dataset" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging_dataset.dataset_id
}

output "bigquery_analytics_dataset" {
  description = "BigQuery analytics dataset ID"
  value       = google_bigquery_dataset.analytics_dataset.dataset_id
}

output "pipeline_service_account_email" {
  description = "The email of the pipeline service account"
  value       = google_service_account.pipeline_sa.email
}
```

---

## Commands to Execute Step 2

```bash
# Step 1: Authenticate with GCP via Google Cloud SDK
gcloud auth application-default login

# Step 2: Navigate to infrastructure directory
cd /Users/abhishekshankar/PROJECTS/DataNexus/infrastructure

# Step 3: Initialize Terraform (downloads Google provider plugin)
terraform init

# Step 4: Preview planned changes
terraform plan

# Step 5: Provision resources on GCP
terraform apply

# Step 6: Verify created resources
gcloud storage ls
gcloud alpha bq datasets list
```
