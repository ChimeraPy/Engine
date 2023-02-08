<script lang="ts">
	import { networkStore } from '$lib/stores';

	import NetworkView from '$lib/Components/NetworkView/NetworkView.svelte';

	const WORKERS_PER_PAGE = 10;

	setInterval(() => {
		networkStore.update((network: App.Manager | {}) => {
			let networkStatus = network as App.Manager;
			if (networkStatus.workers?.length) {
				const worker = networkStatus.workers[0];
				worker.nodes.map((node) => {
					node.running = !node.running;
				});
				console.log(network.workers[0].nodes);
			}
			return network;
		});
	}, 2000);
</script>

<NetworkView manager={$networkStore} workersPerPage={WORKERS_PER_PAGE} />
