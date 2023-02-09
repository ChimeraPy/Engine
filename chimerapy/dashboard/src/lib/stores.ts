import { readable, writable } from 'svelte/store';
import type { Writable } from 'svelte/store';
import { MockApi } from './Services/MockApi.js';

const api = new MockApi('https://localhost:5173/');

export const networkStore = writable<App.Manager | {}>({});

export async function initNetwork(
	fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
) {
	api.getNetworkMap(fetch).then((network) => {
		networkStore.set(network);
	});
}
