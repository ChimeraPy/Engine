<script lang="ts">
	import { Footer, FooterCopyright } from 'flowbite-svelte';
	export let copyrightHolder, copyrightHolderURL;
</script>

<Footer>
	<div class="mx-auto">
		<FooterCopyright href={copyrightHolderURL} by={copyrightHolder} year={2023} target="_blank" />
	</div>
</Footer>
