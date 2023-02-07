// See https://kit.svelte.dev/docs/types#app
// for information about these interfaces
declare global {
	namespace App {
		// interface Error {}
		// interface Locals {}
		// interface PageData {}
		// interface Platform {}
		interface Worker {}
		interface Node {
			name: string;
			id: string;
		}
		interface Graph {}

		interface Manager {
			ip: string; // Define proper IP
			port: number;
			workers: Worker[];
			nodes: Node[];
			graph: Graph;
			worker_graph_map: { string: string }[];
		}
	}
}

export {};
