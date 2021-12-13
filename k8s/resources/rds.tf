resource "aws_db_instance" "vinipostgresql-instance" {
  identifier             = "vinipostgresql-instance"
  name                   = "vinipostgresql"
  instance_class         = "db.t2.micro"
  allocated_storage      = 5
  engine                 = "postgres"
  engine_version         = "12.5"
  skip_final_snapshot    = true
  publicly_accessible    = true
  vpc_security_group_ids = [aws_security_group.vinipostgresql.id]
  username               = var.postgres_user
  password               = var.postgres_user

  tags = {
    tag-key = "vini-cluster-postgres-etl-aws"
  }
}

resource "aws_security_group" "vinipostgresql" {
  vpc_id = aws_vpc.redshift_vpc.id

  ingress {
    from_port = 0
    to_port   = 5432
    protocol  = "tcp"
  }

  tags = {
    tag-key = "sg-postgres"
  }
}