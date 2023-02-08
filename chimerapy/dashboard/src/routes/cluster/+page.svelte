<script lang="ts">
	import { network } from '$lib/stores';

    import NetworkView from "$lib/Components/NetworkView/NetworkView.svelte";
    const WORKERS_PER_PAGE = 10;
</script>

<NetworkView manager="{$network}" workersPerPage="{WORKERS_PER_PAGE}"/>
