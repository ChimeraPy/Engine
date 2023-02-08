<script lang="ts">
	export let node: App.Node, index: number;
	import { objectDetails } from '$lib/utils';
	import { Badge } from 'flowbite-svelte';
</script>

<div class="flex p-2 flex-row md:justify-between">
	<div>
		{objectDetails(node, index)}
	</div>
	<div>
		<Badge large border role="button" color="blue">Feed</Badge>
		<Badge large border color={node.running ? 'green' : 'red'}
			>{node.running ? 'online' : 'offline'}</Badge
		>
	</div>
</div>
<hr />
