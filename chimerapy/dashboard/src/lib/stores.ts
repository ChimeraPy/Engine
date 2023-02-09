import { dev } from '$app/environment';
import { writable } from 'svelte/store';
import { MockApi } from './Services/MockApi.js';
import type { Manager } from './models';

const URL = dev ? '/api' : '';

const api = new MockApi(URL);

// ToDo: this store is fine for now and distributed async requires the tree to be regenerated in realtime. But something diff based maybe?
export const networkStore = writable<Manager | null>(null);

export async function initNetwork(
	fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
) {
	api.getNetworkMap(fetch).then((network) => {
		networkStore.set(network);
	});
}
