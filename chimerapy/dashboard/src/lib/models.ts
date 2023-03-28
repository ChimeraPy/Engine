// ToDo: A lot of interface modeling needs to happen here.

export enum ManagerStatus {
	STARTED = 'STARTED',
	RUNNING = 'RUNNING',
	WORKERS_READY = 'WORKERS_READY',
	PIPELINE_READY = 'PIPELINE_READY',
	PAUSED = 'PAUSED',
	STOPPED = 'STOPPED'
}

export interface Node {
	ip: string;
	name: string;
	id: string;
	port: number;
	running: boolean;
	data_type: 'Video' | 'Series' | 'Audio';
	dashboard_component: null | string;
}

export interface Worker {
	ip: string;
	name: string;
	id: string;
	port: number;
	nodes: Node[];
}

export interface Graph {
	id: string;
}

export interface Manager {
	ip: string; // Define proper IP
	status: ManagerStatus;
	port: number;
	workers: Worker[];
	graph?: Graph;
	worker_graph_map?: { string: string }[];
	network_updates_port: number;
}
