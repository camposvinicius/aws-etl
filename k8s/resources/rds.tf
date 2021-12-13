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

data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "vinipostgresql" {
  vpc_id = data.aws_vpc.default.id
  name   = "vinipostgresql"

  tags = {
    tag-key = "sg-postgres"
  }
}

resource "aws_security_group_rule" "ingress_all" {
  type              = "ingress"
  from_port         = 0
  to_port           = 5432
  protocol          = "tcp"
  security_group_id = aws_security_group.vinipostgresql.id

  depends_on = [
    aws_security_group.vinipostgresql
  ]
}