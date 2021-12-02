resource "aws_redshift_cluster" "default" {
  cluster_identifier  = "tf-redshift-cluster"
  database_name       = "etlvini"
  master_username     = "vini"
  master_password     = "Etl-vini-aws-1"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true
}