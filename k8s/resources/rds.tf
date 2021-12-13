resource "aws_db_instance" "vinipostgresql-instance" {
  identifier             = "vinipostgresql-instance"
  name                   = "vinipostgresql"
  instance_class         = "db.t2.micro"
  allocated_storage      = 5
  engine                 = "postgres"
  engine_version         = "12.5"
  skip_final_snapshot    = true
  publicly_accessible    = true
  vpc_security_group_ids = [data.aws_vpc.postgres.id]
  username               = var.postgres_user
  password               = var.postgres_user

  tags = {
    tag-key = "vini-cluster-postgres-etl-aws"
  }
}