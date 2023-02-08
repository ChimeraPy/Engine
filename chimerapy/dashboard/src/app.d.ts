// See https://kit.svelte.dev/docs/types#app
// for information about these interfaces
declare global {
	namespace App {
		// interface Error {}
		// interface Locals {}
		// interface PageData {}
		// interface Platform {}

		interface Node {
			name: string;
			id: string;
			port: number;
			running: boolean;
			data_type: 'Video' | 'Series' | 'Audio';
			dashboard_component: null | string;
		}

		interface Worker {
			ip: string;
			name: string;
			id: string;
			port: number;
			nodes: Node[];
		}

		interface Manager {
			ip: string; // Define proper IP
			port: number;
			workers: Worker[];
			graph?: Graph;
			worker_graph_map?: { string: string }[];
		}
	}
}

export {};
