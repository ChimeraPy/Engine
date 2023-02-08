import { readable, writable } from 'svelte/store';
import { MockApi } from './Services/MockApi.js';

const api = new MockApi('https://localhost:5173/');

export const network = readable({}, (set) => {
	api.getNetworkMap().then((manager) => set(manager));
});

export async function initNetwork() {

}
