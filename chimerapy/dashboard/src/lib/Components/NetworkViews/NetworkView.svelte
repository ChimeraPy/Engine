<script lang="ts">
	import { AccordionItem, Accordion, Spinner } from 'flowbite-svelte';
	import { networkEntityDetails } from '$lib/utils';
	import Node from './Node.svelte';
	import type { Manager } from '$lib/models';

	let running = false;
	export let manager: Manager;
</script>

{#if !manager}
	<Spinner />
{:else}
	<Accordion active class="w-100">
		{#each manager?.workers || [] as worker, i}
			<AccordionItem class="font-semibold gap-2">
				<div slot="header">{networkEntityDetails(worker, i)}</div>
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
{/if}

<!--ToDo: Implement Pagination-->
