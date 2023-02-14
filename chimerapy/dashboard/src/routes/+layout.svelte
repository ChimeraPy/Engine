<script lang="ts">
	import '../app.postcss';
	import Header from '$lib/Components/Header/Header.svelte';
	import Footer from '$lib/Components/Footer/Footer.svelte';
	export let data;
</script>

<div class="container mx-auto flex flex-col h-screen">
	<Header sections={data.sections} logo={data.logo} />
	<main class="mb-auto h-full">
		<slot />
	</main>
	<Footer copyrightHolder={data.copyrightHolder} copyrightHolderURL={data.copyrightHolderURL} />
</div>
