# 📍 AWS Cloud Setup & Architecture Guide (Amazon Web Services)

---

## 🏗️ AWS vs GCP Technology Mapping for DataNexus

| Component Layer | GCP Technology | AWS Technology | Role in DataNexus |
|---|---|---|---|
| **Data Lake** | Google Cloud Storage (GCS) | **Amazon S3** | Medallion Lake (`bronze/`, `silver/`, `gold/`) |
| **Data Warehouse / OLAP** | Google BigQuery | **AWS Glue Catalog + Amazon Athena / Redshift** | Serverless SQL & Star Schema analytics |
| **Metadata Catalog** | BigQuery Metastore | **AWS Glue Data Catalog** | Schema registry & table definitions |
| **Batch/Stream Compute** | Spark on GKE / Dataproc | **Apache Spark on Amazon EMR / Docker / Glue** | Distributed ETL & PySpark transformations |
| **IAM & Security** | GCP IAM Service Account | **AWS IAM Role & Policies** | Least-privilege access for data pipelines |
| **IaC** | Terraform (Google Provider) | **Terraform (AWS Provider)** | Automated infrastructure provisioning |

---

## 📁 AWS Terraform File Structure

```
DataNexus/
└── infrastructure/
    └── aws/
        ├── variables.tf       <-- AWS Region, Bucket Name, Glue Database names
        ├── main.tf            <-- AWS Provider configuration & IAM Role
        ├── s3.tf              <-- S3 Data Lake (Bronze/Silver/Gold) + Lifecycle rules
        ├── glue_athena.tf     <-- Glue Catalog (Raw/Staging/Analytics) & Athena Workgroup
        └── outputs.tf         <-- Exported S3 bucket names, Glue DBs, IAM ARNs
```

---

## 💻 Commands to Provision AWS Infrastructure

### 1. Configure AWS CLI with your Access Keys
```bash
aws configure
# AWS Access Key ID: <Your_Access_Key>
# AWS Secret Access Key: <Your_Secret_Key>
# Default region name: us-east-1
# Default output format: json
```

### 2. Navigate to the AWS Terraform Folder
```bash
cd /Users/abhishekshankar/PROJECTS/DataNexus/infrastructure/aws
```

### 3. Initialize Terraform for AWS
```bash
terraform init
```

### 4. Preview the AWS Cloud Plan (Dry Run)
```bash
terraform plan
```

### 5. Provision AWS Cloud Resources
```bash
terraform apply
```
*(Type `yes` when prompted)*

### 6. Verify AWS Cloud Resources via AWS CLI
```bash
# Verify S3 Data Lake Bucket
aws s3 ls

# Verify AWS Glue Databases
aws glue get-databases
```
