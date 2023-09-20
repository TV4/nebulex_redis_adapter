defmodule NebulexRedisAdapter.PrimaryReplicaTest do
  use ExUnit.Case, async: true
  use NebulexRedisAdapter.CacheTest
  use Mimic

  import Nebulex.CacheCase, only: [with_telemetry_handler: 3]

  alias NebulexRedisAdapter.TestCache.PrimaryReplica, as: Cache

  setup do
    {:ok, pid} = Cache.start_link()
    _ = Cache.delete_all()

    on_exit(fn ->
      :ok = Process.sleep(100)

      if Process.alive?(pid), do: Cache.stop(pid)
    end)

    {:ok, cache: Cache, name: Cache}
  end

  test "error: missing :redis_cluster option" do
    defmodule PrimaryReplicaWithInvalidOpts do
      @moduledoc false
      use Nebulex.Cache,
        otp_app: :nebulex_redis_adapter,
        adapter: NebulexRedisAdapter
    end

    _ = Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{message: msg}, _}} =
             PrimaryReplicaWithInvalidOpts.start_link(mode: :primary_replica)

    assert Regex.match?(~r/invalid value for :primary_replica option: expected non-empty/, msg)
  end
end
