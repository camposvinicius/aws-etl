resource "aws_redshift_cluster" "default" {
  cluster_identifier  = "tf-redshift-cluster"
  database_name       = "etlvini"
  master_username     = "vini"
  master_password     = "Etl-vini-aws-1"
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  skip_final_snapshot = true
  publicly_accessible = true
  iam_roles           = ["role_redshift"]

  depends_on = [
    aws_iam_role.role_redshift
  ]
}

resource "aws_iam_role" "role_redshift" {
  name               = "role_redshift"
  assume_role_policy = resource.aws_iam_policy.redshift_policy

  depends_on = [
    aws_iam_policy.redshift_policy
  ]
}

resource "aws_iam_policy" "redshift_policy" {
  name = "redshift_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:Get*",
          "s3:List*"
        ]
        Resource = "*"
      }
    ]
  })
}