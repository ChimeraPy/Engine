<script lang="ts">
	import type { Node } from '$lib/models';

	export let node: Node, index: number, ip: string;
	$: node.ip = ip;
	import { networkEntityDetails } from '$lib/utils';
	import { Badge } from 'flowbite-svelte';
</script>

<div class="flex p-2 flex-row md:justify-between">
	<div>
		{networkEntityDetails(node, index, 'Node')}
	</div>
	<div>
		<Badge large border role="button" color="blue">Feed</Badge>
		<Badge large border color={node.ready ? 'green' : 'red'}
			>{node.ready ? 'ready' : 'offline'}</Badge
		>
	</div>
</div>
<hr />
