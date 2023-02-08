import type { writable } from 'svelte/store';

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
	): Promise<App.Manager> {
		return (await (await fetch('/mocks/networkMap.json')).json()) as App.Manager;
	}
}
