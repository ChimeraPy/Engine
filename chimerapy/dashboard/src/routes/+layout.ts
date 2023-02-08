export const ssr = false;

export function load() {
	return {
		sections: [
			{ slug: '/pipeline', title: 'Pipeline Design' },
			{ slug: '/cluster', title: 'Cluster Control' },
			{ slug: '/dashboard', title: 'Data Dashboard' },
			{ slug: '/help', title: 'Help' },
			{ slug: '/about', title: 'About' }
		]
	};
}
