const defaultTheme = require('tailwindcss/defaultTheme');

const config = {
	content: [
		'./src/**/*.{html,js,svelte,ts}',
		'./node_modules/flowbite-svelte/**/*.{html,js,svelte,ts}'
	],

	theme: {
		extend: {
			fontFamily: {
				poppins: ['Poppins', ...defaultTheme.fontFamily.sans]
			}
		}
	},

	plugins: [require('flowbite/plugin'), require('tailwind-scrollbar')],
	darkMode: 'class'
};

module.exports = config;
