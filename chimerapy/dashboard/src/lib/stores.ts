import { dev } from '$app/environment';
import { writable } from 'svelte/store';
import { NetworkApi } from './Services/NetworkApi';
import type { ResponseError } from './Services/NetworkApi';
import type { Manager } from './models';

const URL = dev ? '/api' : '';

const api = new NetworkApi(URL);

// ToDo: this store is fine for now and distributed async requires the tree to be regenerated in realtime. But something diff based maybe?
export const networkStore = writable<Manager | null>(null);
export const errorStore = writable<ResponseError | null>(null);

export async function initNetwork(
	fetch: (input: RequestInfo | URL, init?: RequestInit | undefined) => Promise<Response>
) {
	api.getNetworkMap(fetch).then((result) => {
		if (result.ok().isSome()) {
			networkStore.set(result.unwrap());
			errorStore.set(null);
		} else {
			errorStore.set(result.unwrap());
			networkStore.set(null);
		}
	});
}
