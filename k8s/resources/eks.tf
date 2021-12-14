module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  cluster_name    = var.cluster_name
  cluster_version = "1.21"
  subnets         = module.vpc.private_subnets

  tags = {
    Vini = "ETL-AWS"
  }

  vpc_id = module.vpc.vpc_id

  workers_group_defaults = {
    root_volume_type = "gp2"
  }

  worker_groups = [
    {
      name                          = "worker-group-1"
      instance_type                 = "t2.xlarge"
      asg_desired_capacity          = 2
      additional_security_group_ids = [aws_security_group.worker_group_mgmt_one.id]
    },
    {
      name                          = "worker-group-2"
      instance_type                 = "t2.large"
      asg_desired_capacity          = 1
      additional_security_group_ids = [aws_security_group.worker_group_mgmt_two.id]
    },
  ]
}

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}

resource "aws_iam_role_policy_attachment" "workers_autoscaling" {
  policy_arn = aws_iam_policy.worker_autoscaling.arn
  role       = module.eks.worker_iam_role_name
}

resource "aws_iam_policy" "worker_autoscaling" {
  name_prefix = "eks-worker-autoscaling-${module.eks.cluster_id}"
  description = "EKS worker node autoscaling policy for cluster ${module.eks.cluster_id}"
  policy      = data.aws_iam_policy_document.worker_autoscaling.json
}

data "aws_iam_policy_document" "worker_autoscaling" {
  statement {
    sid    = "eksWorkerAutoscalingAll"
    effect = "Allow"

    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:DescribeTags",
      "ec2:DescribeLaunchTemplateVersions",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "eksWorkerAutoscalingOwn"
    effect = "Allow"

    actions = [
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
      "autoscaling:UpdateAutoScalingGroup",
    ]

    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "autoscaling:ResourceTag/kubernetes.io/cluster/${module.eks.cluster_id}"
      values   = ["owned"]
    }

    condition {
      test     = "StringEquals"
      variable = "autoscaling:ResourceTag/k8s.io/cluster-autoscaler/enabled"
      values   = ["true"]
    }
  }
}