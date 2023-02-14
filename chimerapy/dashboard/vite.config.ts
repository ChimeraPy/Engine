import { sveltekit } from '@sveltejs/kit/vite';
import type { UserConfig } from 'vite';

const config: UserConfig = {
	plugins: [sveltekit()],
	define: {
		'process.env': {}
	},
	test: {
		include: ['src/**/*.{test,spec}.{js,ts}'],
		environment: 'jsdom'
	},
	server: {
		proxy: {
			'/api': {
				target: 'http://129.59.104.153:9001',
				changeOrigin: true,
				secure: false,
				rewrite: (path: string) => path.replace('/api', '')
			}
		}
	}
};

export default config;
