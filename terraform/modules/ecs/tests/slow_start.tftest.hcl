# A task that has just become healthy still has a cold JVM, and one given a
# full share of load 27 seconds after starting missed its store timeout. The
# target group ramps new tasks up; these plan the module against a mocked AWS
# provider, so they need no credentials: `make tf-test`.

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
  alb_ingress_cidrs   = ["203.0.113.7/32"]
}

run "ramps_a_new_task_up_by_default" {
  command = plan

  assert {
    condition     = aws_lb_target_group.app.slow_start == 60
    error_message = "A new task should be ramped up over 60 seconds by default"
  }
}

run "can_be_turned_off" {
  command = plan

  variables {
    slow_start_seconds = 0
  }

  assert {
    condition     = aws_lb_target_group.app.slow_start == 0
    error_message = "0 should turn slow start off"
  }
}

run "refuses_a_duration_the_alb_does_not_accept" {
  command = plan

  variables {
    slow_start_seconds = 10
  }

  expect_failures = [var.slow_start_seconds]
}
