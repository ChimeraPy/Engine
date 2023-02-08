import type { PageLoad } from './$types';
import { initNetwork } from '$lib/stores';

export const load: PageLoad = ({ fetch }) => {
	initNetwork(fetch);
};
