<script lang="ts">
	import { networkStore } from '$lib/stores';

	import NetworkView from '$lib/Components/NetworkView/NetworkView.svelte';

	const WORKERS_PER_PAGE = 10;
</script>

<NetworkView manager={$networkStore} workersPerPage={WORKERS_PER_PAGE} />
