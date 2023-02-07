import { readable, writable } from 'svelte/store';
import { MockApi } from './Services/MockApi.js';

const api = new MockApi('https://localhost:8080');

export const network = readable({}, (set) => {
	api.getNetwork().then((manager) => set(manager));
});

export async function initNetwork() {}
