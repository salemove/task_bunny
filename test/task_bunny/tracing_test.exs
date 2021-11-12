defmodule TaskBunny.TracingTest do
  use ExUnit.Case, async: false
  import TaskBunny.QueueTestHelper
  alias TaskBunny.{Worker, Queue, JobTestHelper}
  alias JobTestHelper.TestJob

  require OpenTelemetry.Tracer
  require Record

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry/include/otel_span.hrl") do
    Record.defrecord(name, spec)
  end

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry_api/include/opentelemetry.hrl") do
    Record.defrecord(name, spec)
  end

  @queue "task_bunny.tracing_test"

  defp all_queues do
    Queue.queue_with_subqueues(@queue)
  end

  defp start_worker() do
    start_worker(%{concurrency: 1, store_rejected_jobs: true})
  end

  defp start_worker(%{concurrency: concurrency, store_rejected_jobs: store_rejected_jobs}) do
    {:ok, worker} =
      Worker.start_link(
        queue: @queue,
        concurrency: concurrency,
        store_rejected_jobs: store_rejected_jobs
      )

    worker
  end

  defp start_worker(%{concurrency: concurrency}) do
    start_worker(%{concurrency: concurrency, store_rejected_jobs: true})
  end

  defp start_worker(%{store_rejected_jobs: store_rejected_jobs}) do
    start_worker(%{concurrency: 1, store_rejected_jobs: store_rejected_jobs})
  end

  setup do
    clean(all_queues())
    JobTestHelper.setup()
    Queue.declare_with_subqueues(:default, @queue)

    on_exit(fn ->
      JobTestHelper.teardown()
    end)

    :application.stop(:opentelemetry)
    :application.set_env(:opentelemetry, :tracer, :otel_tracer_default)

    :application.set_env(:opentelemetry, :processors, [
      {:otel_batch_processor, %{scheduled_delay_ms: 1, exporter: {:otel_exporter_pid, self()}}}
    ])

    :application.start(:opentelemetry)

    :ok
  end

  test "creates span for publisher and for consumer" do
    worker = start_worker()
    TestJob.enqueue(%{"hello" => "world"}, queue: @queue)
    JobTestHelper.wait_for_perform()

    # Send span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test send",
                      kind: :producer,
                      status: :undefined,
                      trace_id: trace_id,
                      parent_span_id: :undefined,
                      span_id: sender_span_id,
                      attributes: attributes
                    )}

    assert [
             "messaging.destination": "",
             "messaging.destination_kind": "queue",
             "messaging.rabbitmq_routing_key": "task_bunny.tracing_test",
             "messaging.system": "rabbitmq"
           ] == List.keysort(attributes, 0)

    # Process span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test process",
                      kind: :consumer,
                      trace_id: ^trace_id,
                      parent_span_id: ^sender_span_id,
                      status: :undefined,
                      attributes: attributes
                    )}

    assert [
             "messaging.destination": "",
             "messaging.destination_kind": "queue",
             "messaging.operation": "process",
             "messaging.rabbitmq_routing_key": "task_bunny.tracing_test",
             "messaging.system": "rabbitmq"
           ] == List.keysort(attributes, 0)

    GenServer.stop(worker)
  end

  test "creates a new root trace if tracing headers are not present" do
    worker = start_worker()
    TestJob.enqueue(%{"hello" => "world"}, queue: @queue, disable_trace_propagation: true)
    JobTestHelper.wait_for_perform()

    # Process span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test process",
                      kind: :consumer,
                      parent_span_id: :undefined
                    )}

    GenServer.stop(worker)
  end

  test "publisher span is part of the current trace" do
    worker = start_worker()

    OpenTelemetry.Tracer.with_span "test span" do
      TestJob.enqueue(%{"hello" => "world"}, queue: @queue)
    end

    JobTestHelper.wait_for_perform()

    # Test span
    assert_receive {:span,
                    span(
                      name: "test span",
                      trace_id: trace_id,
                      parent_span_id: :undefined,
                      span_id: test_span_id
                    )}

    # Send span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test send",
                      kind: :producer,
                      status: :undefined,
                      trace_id: ^trace_id,
                      parent_span_id: ^test_span_id
                    )}

    GenServer.stop(worker)
  end

  test "spans created inside the worker are part of the job trace" do
    worker = start_worker()
    TestJob.enqueue(%{"create_span" => "true"}, queue: @queue)
    JobTestHelper.wait_for_perform()

    # Process span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test process",
                      kind: :consumer,
                      trace_id: trace_id,
                      span_id: process_span_id
                    )}

    # Job span
    assert_receive {:span,
                    span(
                      name: "job span",
                      kind: :internal,
                      trace_id: ^trace_id,
                      parent_span_id: ^process_span_id
                    )}

    GenServer.stop(worker)
  end

  test "adds error status to span if job throws an exception" do
    worker = start_worker()
    TestJob.enqueue(%{"exception" => "true"}, queue: @queue)
    JobTestHelper.wait_for_perform()

    expected_status = OpenTelemetry.status(:error, "")

    # Process span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test process",
                      kind: :consumer,
                      status: ^expected_status,
                      events: [
                        event(
                          name: "exception",
                          attributes: [
                            {"exception.type", "Elixir.RuntimeError"},
                            {"exception.message", "unexpected error"},
                            {"exception.stacktrace", _stacktrace}
                          ]
                        )
                      ]
                    )}

    GenServer.stop(worker)
  end

  test "adds error status to span if job returns an error" do
    worker = start_worker()
    TestJob.enqueue(%{"fail" => "true"}, queue: @queue)
    JobTestHelper.wait_for_perform()

    expected_status = OpenTelemetry.status(:error, "")

    # Process span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test process",
                      kind: :consumer,
                      status: ^expected_status,
                      events: []
                    )}

    GenServer.stop(worker)
  end

  test "adds error status to span if job is rejected" do
    worker = start_worker()
    TestJob.enqueue(%{"reject" => "true"}, queue: @queue)
    JobTestHelper.wait_for_perform()

    expected_status = OpenTelemetry.status(:error, "")

    # Process span
    assert_receive {:span,
                    span(
                      name: ".task_bunny.tracing_test process",
                      kind: :consumer,
                      status: ^expected_status,
                      events: []
                    )}

    GenServer.stop(worker)
  end
end
