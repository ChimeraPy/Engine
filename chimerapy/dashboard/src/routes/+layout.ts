export const ssr = false;
import type { LayoutLoad } from './$types';
import { initNetwork } from '$lib/stores';

export const load: LayoutLoad = ({ fetch }) => {
	initNetwork(fetch);
	return {
		sections: [
			{ slug: '/pipeline', title: 'Pipeline Design' },
			{ slug: '/cluster', title: 'Cluster Control' },
			{ slug: '/dashboard', title: 'Data Dashboard' },
			{ slug: '/help', title: 'Help' },
			{ slug: '/about', title: 'About' }
		],
		logo: '/ChimeraPy.png',
		copyrightHolderURL: 'https://wp0.vanderbilt.edu/oele/',
		copyrightHolder: 'oele-isis-vanderbilt'
	};
};
