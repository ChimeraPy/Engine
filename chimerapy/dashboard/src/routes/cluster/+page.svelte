<script lang="ts">
	import { networkStore } from '$lib/stores';
	import { ZMQSubscriber } from '$lib/Services/ZMQSubscriber';

	import NetworkView from '$lib/Components/NetworkViews/NetworkView.svelte';
	import ManagerControls from '$lib/Components/NetworkViews/ManagerControls.svelte';
	import LogViewer from '$lib/Components/LogViewer/LogViewer.svelte';
	import { tick } from 'svelte';

	import { onDestroy, onMount } from 'svelte';

	let logs: { id: number; text: string }[] = [];
	let logViewer: LogViewer;

	const MAX_LOGS = 10000;

	let unsubscribe = null;
	onMount(() => {
		const subscriber = new ZMQSubscriber('localhost', 10000, 'mock_logs', 10000, 10, false);

		unsubscribe = subscriber.subscribe(
			async (log) => {
				if (logs.length > MAX_LOGS) {
					logs = [];
				}
				logs = [...logs, { id: logs.length + 1, text: log }];
				await tick();
				if (!logViewer?.isHovered()) {
					logViewer.scrollToBottom();
				}
			},
			(error) => {
				console.error(error);
			}
		);

		logViewer.addScrollClass();
	});

	onDestroy(() => {
		if (unsubscribe) {
			unsubscribe();
		}
	});
</script>

<ManagerControls manager={$networkStore} />
<div
	class="h-3/5 max-h-min overflow-y-auto scrollbar-thin scrollbar-thumb-gray-700 scrollbar-track-gray-100"
>
	<NetworkView manager={$networkStore} />
</div>
<div class="mt-10">
	<LogViewer bind:this={logViewer} {logs} title="Manager Logs" />
</div>
