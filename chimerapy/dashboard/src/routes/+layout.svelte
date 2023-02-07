<script lang="ts">
	import '../app.postcss';
	import Header from '$lib/Components/Header/Header.svelte';
	import Footer from '$lib/Components/Footer/Footer.svelte';

	export let data: { string: any };
</script>

<div class="container mx-auto flex flex-col h-screen justify-between">
	<Header sections={data.sections} />
	<main>
		<slot />
	</main>
	<Footer sections={data.sections} />
</div>
