variable "region" {
  default = "us-east-1"
}

variable "cluster_name" {
  default = "vini-cluster-eks"
}

variable "redshift_user" {
  default = "vini"
}
variable "redshift_pass" {
  default = "Etl-vini-aws-1"
}

variable "redshift_db" {
  default = "etlvini"
}

variable "postgres_user" {
  default = "vinietlaws"
}

variable "postgres_pass" {
  default = "vinietlaws"
}

variable "email" {
  default = "test.vini.etl.aws@gmail.com"
}
