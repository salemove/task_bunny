defmodule TaskBunny.ConfigTest do
  use ExUnit.Case, async: false
  alias TaskBunny.Config

  defp test_config do
    [
      queue: [
        namespace: "test.",
        queues: [
          [name: "high", worker: [concurrency: 10], jobs: ["High.*"]],
          [name: "normal", jobs: :default],
          [name: "low", worker: false, jobs: [SlowJob]],
          [name: "disabled", worker: [concurrency: 0], jobs: :default],
          [
            name: "storing-rejected-disabled",
            worker: [store_rejected_jobs: false],
            jobs: :default
          ]
        ]
      ],
      extra_queue: [
        namespace: "extra.",
        queues: [
          [name: "queue1", worker: [concurrency: 1], jobs: ["Extra.*"], host: :extra]
        ]
      ]
    ]
  end

  setup do
    :meck.new(Application, [:passthrough])
    :meck.expect(Application, :get_all_env, fn :task_bunny -> test_config() end)

    on_exit(fn -> :meck.unload() end)

    :ok
  end

  describe "queues" do
    test "combines all queue configs and sets namespace" do
      queue_names =
        Config.queues()
        |> Enum.map(fn queue -> queue[:name] end)

      assert queue_names == [
               "test.high",
               "test.normal",
               "test.low",
               "test.disabled",
               "test.storing-rejected-disabled",
               "extra.queue1"
             ]
    end
  end

  describe "workers" do
    test "lists up information for workers" do
      assert Config.workers() == [
               [
                 queue: "test.high",
                 concurrency: 10,
                 store_rejected_jobs: true,
                 host: :default,
                 jobs: ["High.*"]
               ],
               [
                 queue: "test.normal",
                 concurrency: 2,
                 store_rejected_jobs: true,
                 host: :default,
                 jobs: :default
               ],
               [
                 queue: "test.storing-rejected-disabled",
                 concurrency: 2,
                 store_rejected_jobs: false,
                 host: :default,
                 jobs: :default
               ],
               [
                 queue: "extra.queue1",
                 concurrency: 1,
                 store_rejected_jobs: true,
                 host: :extra,
                 jobs: ["Extra.*"]
               ]
             ]
    end
  end

  describe "queue_for_job" do
    defp queue_for_job(job) do
      Config.queue_for_job(job)[:name]
    end

    test "returns matched queue" do
      assert queue_for_job(High.TestJob) == "test.high"
      assert queue_for_job(SlowJob) == "test.low"
      assert queue_for_job(SampleJob) == "test.normal"
      assert queue_for_job(Extra.TestJob) == "extra.queue1"
      assert queue_for_job(Foo.High.TestJob) == "test.normal"
    end
  end

  describe "accepts_job?" do
    test "accepts the jobs matching the configured jobs" do
      assert Config.accepts_job?([SlowJob], SlowJob)
      assert Config.accepts_job?(SlowJob, SlowJob)
      assert Config.accepts_job?(["High.*"], High.TestJob)
      assert Config.accepts_job?([SlowJob, "High.*"], High.TestJob)
    end

    test "does not accept the jobs not matching the configured jobs" do
      refute Config.accepts_job?([SlowJob], SampleJob)
      refute Config.accepts_job?(SlowJob, SampleJob)
      refute Config.accepts_job?(["High.*"], Foo.High.TestJob)
      refute Config.accepts_job?([], SlowJob)
    end

    test "accepts any job for the default queue and for a queue without jobs" do
      assert Config.accepts_job?(:default, SampleJob)
      assert Config.accepts_job?(nil, SampleJob)
    end
  end

  describe "publisher_pool_size" do
    test "returns the pool size for the publisher if it has been configured for the application" do
      :meck.expect(Application, :get_env, fn :task_bunny, :publisher_pool_size, 15 -> 5 end)

      assert Config.publisher_pool_size() == 5
    end

    test "returns 15 by default" do
      assert Config.publisher_pool_size() == 15
    end
  end

  describe "publisher_max_overflow" do
    test "returns the max overflow for the publisher if is configured for the application" do
      :meck.expect(Application, :get_env, fn :task_bunny, :publisher_max_overflow, 0 -> 5 end)

      assert Config.publisher_max_overflow() == 5
    end

    test "returns 0 by default" do
      assert Config.publisher_max_overflow() == 0
    end
  end
end
