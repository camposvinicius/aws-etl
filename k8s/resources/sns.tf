resource "aws_sns_topic" "mysns" {
  name = "send-email"
}

resource "aws_sns_topic_subscription" "send-email" {
  topic_arn = aws_sns_topic.mysns.arn
  protocol  = "email"
  endpoint  = var.email

  depends_on = [
    aws_sns_topic.mysns
  ]
}

data "aws_iam_policy_document" "sns_topic_policy" {
  policy_id = "__default_policy_ID"

  statement {
    actions = [
      "SNS:Publish"
    ]

    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      aws_sns_topic.mysns.arn,
    ]

    sid = "__default_statement_ID"
  }
}