<script lang="ts">
	import '../app.postcss';
	import Header from '$lib/Components/Header/Header.svelte';
    import Footer from '$lib/Components/Footer/Footer.svelte';

    export let data: {string: any};
</script>


<div class="container mx-auto">
<Header sections={data.sections}/>
<slot></slot>
</div>
<Footer sections="{data.sections}"/>
