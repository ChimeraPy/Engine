import type { Manager } from '$lib/models';

class Api {
	url: string;

	constructor(url: string) {
		this.url = url;
	}
}

export class MockApi extends Api {
	constructor(url: string) {
		super(url);
	}

	async getNetworkMap(
		fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
	): Promise<Manager> {
		return (await (await fetch('/mocks/networkMap.json')).json()) as Manager;
	}

	async load(
		fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
	) {
		console.log(await (await fetch(`${this.url}/network`)).json());
	}
}
