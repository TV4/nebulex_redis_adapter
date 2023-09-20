defmodule NebulexRedisAdapter.PrimaryReplica do
  # Client-side Cluster
  @moduledoc false

  import NebulexRedisAdapter.Helpers

  alias NebulexRedisAdapter.PrimaryReplica.Supervisor, as: PrimaryReplicaSupervisor
  alias NebulexRedisAdapter.{Options, Pool}

  @typedoc "Proxy type to the adapter meta"
  @type adapter_meta :: Nebulex.Adapter.metadata()

  @type node_entry :: {node_name :: atom, node_type :: atom, pool_size :: pos_integer}
  @type nodes_config :: [node_entry]

  ## API

  @spec init(adapter_meta, Keyword.t()) :: {Supervisor.child_spec(), adapter_meta}
  def init(%{name: name, registry: registry, pool_size: pool_size} = adapter_meta, opts) do
    cluster_opts = Keyword.get(opts, :primary_replica)

    if is_nil(cluster_opts) do
      raise ArgumentError,
            Options.invalid_cluster_config_error(
              "invalid value for :primary_replica option: ",
              nil,
              :primary_replica
            )
    end

    {node_connections_specs, nodes} =
      cluster_opts
      |> Keyword.fetch!(:nodes)
      |> Enum.reduce({[], []}, fn {node_name, node_opts}, {acc1, acc2} ->
        node_opts =
          node_opts
          |> Keyword.put(:name, name)
          |> Keyword.put(:registry, registry)
          |> Keyword.put(:node, node_name)
          |> Keyword.put_new(:pool_size, pool_size)

        child_spec =
          Supervisor.child_spec({PrimaryReplicaSupervisor, node_opts},
            type: :supervisor,
            id: {name, node_name}
          )

        {[child_spec | acc1],
         [
           {node_name, Keyword.fetch!(node_opts, :type), Keyword.fetch!(node_opts, :pool_size)}
           | acc2
         ]}
      end)

    node_connections_supervisor_spec = %{
      id: :node_connections_supervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [node_connections_specs, [strategy: :one_for_one]]}
    }

    # Update adapter meta
    adapter_meta = Map.put(adapter_meta, :nodes, nodes)

    {node_connections_supervisor_spec, adapter_meta}
  end

  @spec exec!(
          Nebulex.Adapter.adapter_meta(),
          Redix.command(),
          Keyword.t(),
          init_acc :: any,
          reducer :: (any, any -> any)
        ) :: any | no_return
  def exec!(
        %{name: name, registry: registry, nodes: nodes},
        command,
        opts,
        _init_acc \\ nil,
        _reducer \\ fn res, _ -> res end
      ) do
    node_type = Keyword.fetch!(opts, :"$operation")
    registry
    |> get_conn(name, nodes, node_type)
    |> Redix.command!(command, redis_command_opts(opts))
  end

  @spec get_conn(atom, atom, nodes_config, atom) :: pid
  def get_conn(registry, name, nodes, operation) do
    {node_name, ^operation, pool_size} = Enum.find(nodes, &match?({_, ^operation, _}, &1))

    Pool.get_conn(registry, {name, operation, node_name}, pool_size)
  end
end
