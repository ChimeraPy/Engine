import type { Manager } from '$lib/models';
import { Err, Ok } from 'ts-monads';

interface Pipeline {}

export interface ResponseError {
	message: string;
	code: number;
}

class Api {
	url: string;

	constructor(url: string) {
		this.url = url;
	}
}

export class NetworkApi extends Api {
	constructor(url: string) {
		super(url);
	}

	async getNetworkMap(
		fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
	): Promise<Ok<Manager> | Err<ResponseError>> {
		const res = await fetch('/mocks/networkMap.json');
		if (res.ok) {
			return new Ok(await res.json());
		} else {
			return new Err({ message: res.statusText, code: res.status });
		}
	}

	async load(
		fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
	) {
		console.log(await (await fetch(`${this.url}/network`)).json());
	}

	async subscribeToLogsZMQ(ip: string, port: number, callable: (e: MessageEvent) => void) {
		const ws = new WebSocket(`ws://localhost:${port}/logs`);
		ws.onmessage = (e) => {
			callable(e);
		};

		ws.onerror = (e) => {
			console.error(e);
		};

		const close = () => {
			ws.close();
		};

		return close;
	}

	async createPipeline() {}

	async deletePipeline() {}
}
