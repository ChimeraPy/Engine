# Development RoadMap for ChimeraPy-Dashboard/ API Planning

This is a rough road map for developing dashboard for ChimeraPy. Eventually, this will be formalized but this is a quick
and dirty version (per se).

Currently, the challenge is how to think about different nodes and their components. So, a nice roadmap with API Schema
and corresponding typescript components should be made possible. There are several possibilities, however, the straight
forward way to do it is to gather the network tree at once is a svelte store and eventual changes in the tree should be
populated by a message passing interface from the server.

But, what does the data look like. And what are the routes?

This is an ongoing/evolving thought process. However, Lets look at the entities in our Network. Note that the graph is a
separate entity and should be treated as such. i.e. A manager will have access to all the global nodes that can be used
to create a pipeline.

## Manager

### States

A manager can be in the following states (we can name them accordingly):

1. Started (waiting for workers to connected) / The dashboard will only start after this.
2. All the workers are connected.
3. Executing a pipeline
4. Stopped and finalizing bookkeeping

### Properties

```typescript
enum ManagerState {
	STARTED = 0, // Just Launched
	WORKERS_CONNECTED = 1, // Few Workers are connected
	NODES_READY = 2, // Nodes are ready (Graph Commited)
	EXECUTING = 3, // Executing the current pipeline
	STOPPED = 4, // Stop Command has been applied
	FINALIZING = 5 // Finalizing the bookkeeping and datatransfoer
}

interface Manager {
	state: ManagerState;
	ip: string; // Define proper IP
	port: number;
	workers: Worker[]; // ToDo Worker Definitions
	graph?: Graph; // ToDo Graph Definitions
	worker_graph_map?: { string: string }[];
}
```

## Capabilities

1. Boot a worker (and nodes)

More thought is needed here?

## Worker

A worker will have similar properties (is a peer in the distributed network) and can data is only accessible from the
manager.

Currently, I have thought the following properties for a worker:

```typescript
interface Worker {
	ip: string;
	name: string;
	id: string;
	port: number;
	nodes: Node[]; // ToDo NodeDefinition
}
```

## Node

A node is an actual process entity running in the worker. It can be a source / sink node producing/ consuming data but
also might be running heavy computational jobs.

## Properties

```typescript
interface Node {
	name: string;
	id: string;
	port: number;
	running: boolean;
	data_type: 'Video' | 'Series' | 'Audio';
	dashboard_component: null | string;
	capabilities: ('callibrate' | 'start' | 'stop')[];
}
```
