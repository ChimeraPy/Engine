<script lang="ts">
	import { AccordionItem, Accordion } from 'flowbite-svelte';
	import { objectDetails } from '$lib/utils';
	import Node from './Node.svelte';

	export let manager: App.Manager,
		workersPerPage: number = 10;
</script>

<Accordion active class="w-100 m-5 md:m-10">
	<h3 class="p-2 bg-red-500 bg-teal-500 text-white font-bold rounded-t-lg">
		Manager@ {manager.ip}:{manager.port}
	</h3>
	{#each manager?.workers || [] as worker, i}
		<AccordionItem class="font-semibold gap-2">
			<div slot="header">{objectDetails(worker, i)}</div>
			{#if worker.nodes.length}
				{#each worker.nodes as node, i}
					<Node {node} index={i} />
				{/each}
			{:else}
				<p class="mb-2 text-gray-500 dark:text-gray-400">No Active Nodes</p>
			{/if}
		</AccordionItem>
	{/each}
</Accordion>
<!--ToDo: Implement Pagination-->
