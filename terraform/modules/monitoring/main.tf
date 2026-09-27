# Monitoring module - CloudWatch dashboards and alarms
#
# Every app metric here must be one the service actually emits, with the
# dimensions it emits. scripts/check-monitoring-metrics.sh checks the names
# and the Environment dimension in CI. CloudWatch identifies a metric by name
# plus its exact dimension set, so a query that leaves out a dimension the app
# sends matches nothing. That is how this dashboard and the breaker alarm sat
# empty: they queried names the app never sent, and dimensions it never used.

locals {
  # Must match METRICS_NAMESPACE in ../../main.tf; the app adds
  # Environment = METRICS_ENVIRONMENT (var.environment) to every datum.
  namespace = "RateLimiter/${var.environment}"
  # SEARCH needs the namespace quoted because of the '/'.
  search_ns = "\"${local.namespace}\""
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project_name}-${var.environment}"

  dashboard_body = jsonencode({
    widgets = [
      # ECS Metrics Row
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ClusterName", var.ecs_cluster_name, "ServiceName", var.ecs_service_name],
            [".", "MemoryUtilization", ".", ".", ".", "."]
          ]
          period = 300
          stat   = "Average"
          region = data.aws_region.current.name
          title  = "ECS Service Metrics"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ECS", "RunningTaskCount", "ClusterName", var.ecs_cluster_name, "ServiceName", var.ecs_service_name]
          ]
          period = 60
          stat   = "Average"
          region = data.aws_region.current.name
          title  = "Running Tasks"
        }
      },
      # Rate Limit Metrics Row
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 8
        height = 6
        properties = {
          # Emitted with ClientTier and Decision dimensions, so summed over a
          # SEARCH rather than queried without them.
          metrics = [
            [{ id = "allowed", label = "Allowed", expression = "SUM(SEARCH('{${local.search_ns},ClientTier,Decision,Environment} MetricName=\"RateLimitAllowed\" Environment=\"${var.environment}\"', 'Sum', 60))" }],
            [{ id = "rejected", label = "Rejected", expression = "SUM(SEARCH('{${local.search_ns},ClientTier,Decision,Environment} MetricName=\"RateLimitRejected\" Environment=\"${var.environment}\"', 'Sum', 60))" }]
          ]
          period = 60
          region = data.aws_region.current.name
          title  = "Rate Limit Decisions"
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 6
        width  = 8
        height = 6
        properties = {
          metrics = [
            [local.namespace, "rate_limit_check", "Environment", var.environment, { stat = "p50" }],
            ["...", { stat = "p99" }]
          ]
          period = 60
          region = data.aws_region.current.name
          title  = "Rate Limit Latency (ms)"
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 6
        width  = 8
        height = 6
        properties = {
          # One line per reason: circuit_breaker, bulkhead, error.
          metrics = [
            [{ id = "degraded", label = "$${PROP('Dim.reason')}", expression = "SEARCH('{${local.search_ns},Environment,reason} MetricName=\"RateLimitDegraded\" Environment=\"${var.environment}\"', 'Sum', 60)" }]
          ]
          period = 60
          region = data.aws_region.current.name
          title  = "Degraded Decisions"
        }
      },
      # ALB Metrics Row
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", var.alb_arn_suffix, { stat = "Sum" }]
          ]
          period = 60
          region = data.aws_region.current.name
          title  = "ALB Request Count"
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 12
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["AWS/ApplicationELB", "HTTPCode_Target_2XX_Count", "LoadBalancer", var.alb_arn_suffix],
            [".", "HTTPCode_Target_4XX_Count", ".", "."],
            [".", "HTTPCode_Target_5XX_Count", ".", "."]
          ]
          period = 60
          stat   = "Sum"
          region = data.aws_region.current.name
          title  = "ALB Response Codes"
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 12
        width  = 8
        height = 6
        properties = {
          metrics = [
            ["AWS/ApplicationELB", "TargetResponseTime", "LoadBalancer", var.alb_arn_suffix, { stat = "p50" }],
            ["...", { stat = "p99" }]
          ]
          period = 60
          region = data.aws_region.current.name
          title  = "ALB Response Time"
        }
      },
      # Circuit Breaker Row. There is one breaker, on the rate-limit store.
      # The cache widget that sat here charted a metric nothing emits: no
      # cache is wired in.
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 24
        height = 6
        properties = {
          metrics = [
            [local.namespace, "CircuitBreakerState", "CircuitBreaker", "dynamodb-ratelimit", "Environment", var.environment]
          ]
          period = 60
          stat   = "Maximum"
          region = data.aws_region.current.name
          title  = "Circuit Breaker State (0=Closed, 0.5=HalfOpen, 1=Open)"
        }
      }
    ]
  })
}

# CloudWatch Alarms
#
# The alarms exist in every environment; only their notifications depend on
# alarm_sns_topic_arn. They used to exist only when a topic was set, and no
# tfvars file sets one, so no deployment had any alarm at all. Each one states
# how it treats missing data rather than inheriting "missing".
locals {
  alarm_actions = var.alarm_sns_topic_arn != "" ? [var.alarm_sns_topic_arn] : []
}

resource "aws_cloudwatch_metric_alarm" "high_error_rate" {
  alarm_name          = "${var.project_name}-${var.environment}-high-error-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  threshold           = 1
  # No requests means no errors.
  treat_missing_data = "notBreaching"

  metric_query {
    id          = "error_rate"
    expression  = "errors / requests * 100"
    label       = "Error Rate %"
    return_data = true
  }

  metric_query {
    id = "errors"
    metric {
      metric_name = "HTTPCode_Target_5XX_Count"
      namespace   = "AWS/ApplicationELB"
      period      = 300
      stat        = "Sum"
      dimensions = {
        LoadBalancer = var.alb_arn_suffix
      }
    }
  }

  metric_query {
    id = "requests"
    metric {
      metric_name = "RequestCount"
      namespace   = "AWS/ApplicationELB"
      period      = 300
      stat        = "Sum"
      dimensions = {
        LoadBalancer = var.alb_arn_suffix
      }
    }
  }

  alarm_description = "Error rate exceeded 1%"
  alarm_actions     = local.alarm_actions
  ok_actions        = local.alarm_actions

  tags = {
    Name = "${var.project_name}-${var.environment}-high-error-rate"
  }
}

resource "aws_cloudwatch_metric_alarm" "high_latency" {
  alarm_name          = "${var.project_name}-${var.environment}-high-latency"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  metric_name         = "TargetResponseTime"
  namespace           = "AWS/ApplicationELB"
  period              = 60
  extended_statistic  = "p99"
  threshold           = 0.1 # 100ms
  treat_missing_data  = "notBreaching"

  dimensions = {
    LoadBalancer = var.alb_arn_suffix
  }

  alarm_description = "P99 latency exceeded 100ms"
  alarm_actions     = local.alarm_actions
  ok_actions        = local.alarm_actions

  tags = {
    Name = "${var.project_name}-${var.environment}-high-latency"
  }
}

# HealthyHostCount is published per target group. With LoadBalancer alone the
# alarm matched no metric, and its fixed threshold of 2 would have fired
# forever in demo and dev, which run one task.
resource "aws_cloudwatch_metric_alarm" "low_healthy_hosts" {
  alarm_name          = "${var.project_name}-${var.environment}-low-healthy-hosts"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  metric_name         = "HealthyHostCount"
  namespace           = "AWS/ApplicationELB"
  period              = 60
  statistic           = "Minimum"
  threshold           = var.min_healthy_tasks
  # A health metric that stops reporting is itself a failure.
  treat_missing_data = "breaching"

  dimensions = {
    TargetGroup  = var.target_group_arn_suffix
    LoadBalancer = var.alb_arn_suffix
  }

  alarm_description = "Fewer than ${var.min_healthy_tasks} healthy tasks"
  alarm_actions     = local.alarm_actions
  ok_actions        = local.alarm_actions

  tags = {
    Name = "${var.project_name}-${var.environment}-low-healthy-hosts"
  }
}

# The app emits CircuitBreakerState{CircuitBreaker, Environment}, recorded
# after every protected call, open ones included. This alarm had no dimensions,
# so it watched a series that never existed.
resource "aws_cloudwatch_metric_alarm" "circuit_breaker_open" {
  alarm_name          = "${var.project_name}-${var.environment}-circuit-breaker-open"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 2
  datapoints_to_alarm = 1
  metric_name         = "CircuitBreakerState"
  namespace           = local.namespace
  period              = 60
  statistic           = "Maximum"
  threshold           = 1 # 1 = Open
  # Recorded per call: no traffic means no datapoint and nothing to protect.
  treat_missing_data = "notBreaching"

  dimensions = {
    CircuitBreaker = "dynamodb-ratelimit"
    Environment    = var.environment
  }

  alarm_description = "The rate-limit store's circuit breaker is open; decisions follow DEGRADATION_MODE"
  alarm_actions     = local.alarm_actions
  ok_actions        = local.alarm_actions

  tags = {
    Name = "${var.project_name}-${var.environment}-circuit-breaker-open"
  }
}

data "aws_region" "current" {}

# Outputs
output "dashboard_url" {
  value = "https://${data.aws_region.current.name}.console.aws.amazon.com/cloudwatch/home?region=${data.aws_region.current.name}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}
