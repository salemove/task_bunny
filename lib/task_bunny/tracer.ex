defmodule TaskBunny.Tracer do
  # Handles tracing concerns.
  #
  # This module is private to TaskBunny and should not be accessed directly.
  #
  @moduledoc false

  require OpenTelemetry.Tracer

  def with_send_span(exchange, routing_key, block) do
    OpenTelemetry.Tracer.with_span "#{exchange}.#{routing_key} send", %{
      attributes: [
        "messaging.system": "rabbitmq",
        "messaging.rabbitmq_routing_key": routing_key,
        "messaging.destination": exchange,
        "messaging.destination_kind": "queue"
      ],
      kind: :producer
    } do
      headers = inject([])

      try do
        block.(headers)
      rescue
        exception ->
          ctx = OpenTelemetry.Tracer.current_span_ctx()
          OpenTelemetry.Span.record_exception(ctx, exception, __STACKTRACE__, [])
          OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, ""))

          reraise(exception, __STACKTRACE__)
      end
    end
  end

  def start_process_span(meta) do
    headers = normalize_headers(Map.get(meta, :headers, []))
    extract(headers)

    exchange = Map.get(meta, :exchange)
    routing_key = Map.get(meta, :routing_key)

    span_ctx =
      OpenTelemetry.Tracer.start_span("#{exchange}.#{routing_key} process", %{
        attributes: [
          "messaging.system": "rabbitmq",
          "messaging.rabbitmq_routing_key": routing_key,
          "messaging.destination": exchange,
          "messaging.destination_kind": "queue",
          "messaging.operation": "process"
        ],
        kind: :consumer
      })

    OpenTelemetry.Tracer.set_current_span(span_ctx)
  end

  def finish_process_span do
    OpenTelemetry.Tracer.end_span()
  end

  def fail_process_span(job_error = %TaskBunny.JobError{}) do
    ctx = OpenTelemetry.Tracer.current_span_ctx()

    if job_error.exception && job_error.stacktrace do
      OpenTelemetry.Span.record_exception(ctx, job_error.exception, job_error.stacktrace, [])
    end

    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, job_error.reason || ""))
    OpenTelemetry.Tracer.end_span()
  end

  def fail_process_span(reason) do
    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, reason))
    OpenTelemetry.Tracer.end_span()
  end

  # amqp uses `:undefined` if no headers are present
  defp normalize_headers(:undefined), do: []

  # amqp returns headers in [{key, type, value}, ...] format. Convert these
  # into just [{key, value}].
  defp normalize_headers(headers) do
    Enum.map(headers, fn {key, _type, value} -> {key, value} end)
  end

  defp extract(carrier), do: :otel_propagator_text_map.extract(carrier)
  defp inject(carrier), do: :otel_propagator_text_map.inject(carrier)
end
