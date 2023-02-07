<script lang="ts">
	import { AccordionItem, Accordion, Listgroup, ListgroupItem } from 'flowbite-svelte';
	import { network } from '$lib/stores';

	let currentPage = 1;

	function objectDetails(worker: App.Worker, workerIndex: number, prefix = 'Worker'): string {
		const details = [
			prefix,
			`#${workerIndex + 1}`,
			'name = ',
			worker.name,
			' ',
			`${worker.ip}@${worker.port}`
		];
		return details.join(' ');
	}
</script>

<Accordion active class="w-100 m-5 md:m-10">
	<h3 class="p-2 bg-red-500 bg-teal-500 text-white font-bold rounded-t-lg">
		Manager@ {$network.ip}:{$network.port}
	</h3>
	{#each $network.workers || [] as worker, i}
		<AccordionItem class="font-semibold gap-2">
			<div slot="header">{objectDetails(worker, i)}</div>
			{#if worker.nodes.length}
				{#each worker.nodes as node, i}
					<p class="mb-2 text-gray-500 dark:text-gray-400">{objectDetails(node, i, 'Node')}</p>
				{/each}
			{:else}
				<p class="mb-2 text-gray-500 dark:text-gray-400">No Active Nodes</p>
			{/if}
		</AccordionItem>
	{/each}
</Accordion>
