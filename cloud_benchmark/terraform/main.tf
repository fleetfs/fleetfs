provider "google" {
  credentials = file("credentials.json")
  project = var.gcp_project_id
  region = "us-central1"
  zone = "us-central1-a"
  version = "~> 2.10"
}

resource "google_container_cluster" "benchmarking" {
  name = "fleetfs-benchmarking"
  location = "us-central1-a"

  remove_default_node_pool = true
  initial_node_count = 1

  master_auth {
    username = ""
    password = ""

    client_certificate_config {
      issue_client_certificate = false
    }
  }
}

resource "google_container_node_pool" "preemptible_nodes" {
  name       = "preemptible-node-pool"
  cluster    = google_container_cluster.benchmarking.name
  node_count = 0

  autoscaling {
    max_node_count = 10
    min_node_count = 0
  }

  node_config {
    preemptible  = true
    machine_type = "custom-6-5632" # Minimum to get >10Gbit/s worth of networking
    image_type = "UBUNTU"
    disk_size_gb = 20
    local_ssd_count = 1

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  timeouts {
    delete = "30m"
  }
}
