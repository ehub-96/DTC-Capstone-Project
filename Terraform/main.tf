provider "google" {
  credentials = file("#YOUR CREDENTIALS")
  project = "#YOUR PROJECT NAME"
  region  = "# YOUR REGION"
  zone    = "#YOUR ZONE"
}

resource "google_storage_bucket" "YOUR BUCKET" {
  name          = "YOUR BUCKET"
  location      = "YOUR LOCATION"
  force_destroy = true

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "YOUR DATASET" {
  dataset_id = "YOUR DATASET ID"
  location   = "YOUR LOCATION"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_iam_member" "YOUR SERVICE" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${var.service_account_email}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_iam_member" "YOUR SERVICE" {
  project = var.project_id
  role    = "roles/compute.admin"
  member  = "serviceAccount:${var.service_account_email}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_iam_member" "YOUR SERVICE" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.service_account_email}"

  lifecycle {
    prevent_destroy = true
  }
}
