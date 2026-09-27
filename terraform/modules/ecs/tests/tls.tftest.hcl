# The ALB used to serve API keys over plain HTTP to 0.0.0.0/0 (finding E).
# These plan the module against a mocked AWS provider, so they need no
# credentials: `make tf-test`.

mock_provider "aws" {
  mock_data "aws_region" {
    defaults = { name = "us-east-1" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
}

variables {
  project_name        = "gate"
  environment         = "test"
  vpc_id              = "vpc-12345678"
  private_subnet_ids  = ["subnet-11111111", "subnet-22222222"]
  public_subnet_ids   = ["subnet-33333333", "subnet-44444444"]
  container_image     = "123456789012.dkr.ecr.us-east-1.amazonaws.com/gate:test"
  dynamodb_table_arns = ["arn:aws:dynamodb:us-east-1:123456789012:table/test"]
  kinesis_stream_arn  = "arn:aws:kinesis:us-east-1:123456789012:stream/test"
  secrets_manager_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
}

run "refuses_public_plaintext" {
  command = plan

  variables {
    alb_ingress_cidrs = ["0.0.0.0/0"]
  }

  expect_failures = [aws_lb_listener.app]
}

run "serves_plaintext_only_to_the_allowed_cidrs" {
  command = plan

  variables {
    alb_ingress_cidrs = ["203.0.113.7/32"]
  }

  assert {
    condition     = aws_lb_listener.app.default_action[0].type == "forward"
    error_message = "Without a certificate, port 80 should serve the API"
  }
  assert {
    condition     = length(aws_lb_listener.https) == 0
    error_message = "No HTTPS listener without a certificate"
  }
  assert {
    condition = alltrue([
      for rule in aws_security_group.alb.ingress : rule.cidr_blocks == tolist(["203.0.113.7/32"])
    ])
    error_message = "Every ALB ingress rule should be limited to alb_ingress_cidrs"
  }
}

run "public_plaintext_only_when_asked_for" {
  command = plan

  variables {
    alb_ingress_cidrs      = ["0.0.0.0/0"]
    allow_public_plaintext = true
  }

  assert {
    condition     = aws_lb_listener.app.default_action[0].type == "forward"
    error_message = "An explicit opt-in should be allowed to serve plaintext"
  }
}

run "with_a_certificate_serves_https_and_redirects_http" {
  command = plan

  variables {
    alb_ingress_cidrs = ["0.0.0.0/0"]
    certificate_arn   = "arn:aws:acm:us-east-1:123456789012:certificate/test"
  }

  assert {
    condition     = length(aws_lb_listener.https) == 1
    error_message = "A certificate should add the HTTPS listener"
  }
  assert {
    condition     = aws_lb_listener.https[0].protocol == "HTTPS" && aws_lb_listener.https[0].ssl_policy == "ELBSecurityPolicy-TLS13-1-2-2021-06"
    error_message = "The HTTPS listener should use a TLS 1.2+ policy"
  }
  assert {
    condition     = aws_lb_listener.app.default_action[0].type == "redirect" && aws_lb_listener.app.default_action[0].redirect[0].protocol == "HTTPS"
    error_message = "Port 80 should only redirect to HTTPS"
  }
}
