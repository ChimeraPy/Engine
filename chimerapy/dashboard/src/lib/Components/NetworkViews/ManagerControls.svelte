<script lang="ts">
	import type { Manager } from '$lib/models';
	import { Button, Tooltip } from 'flowbite-svelte';
	export let manager: Manager;
	let running = false;
</script>

<div class="flex justify-between bg-gray-900 px-2 py-2 items-center">
	<div>
		<h3 class="text-white font-bold rounded-t-lg">
			Manager@ {manager?.ip}:{manager?.port}
		</h3>
	</div>
	<div>
		<Button pill={true} size="md" class="!p-2" color="green" on:click={() => (running = !running)}>
			{#if !running}
				<svg
					xmlns="http://www.w3.org/2000/svg"
					fill="none"
					viewBox="0 0 24 24"
					stroke-width="1.5"
					stroke="currentColor"
					class="w-6 h-6"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M5.25 5.653c0-.856.917-1.398 1.667-.986l11.54 6.348a1.125 1.125 0 010 1.971l-11.54 6.347a1.125 1.125 0 01-1.667-.985V5.653z"
					/>
				</svg>
			{:else}
				<svg
					xmlns="http://www.w3.org/2000/svg"
					fill="none"
					viewBox="0 0 24 24"
					stroke-width="1.5"
					stroke="currentColor"
					class="w-6 h-6"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M15.75 5.25v13.5m-7.5-13.5v13.5"
					/>
				</svg>
			{/if}
			<Tooltip>Start executing current pipeline</Tooltip>
		</Button>
		<Button pill={true} size="md" class="!p-2 ml-2" color="red" disabled={!running}>
			<svg
				xmlns="http://www.w3.org/2000/svg"
				fill="none"
				viewBox="0 0 24 24"
				stroke-width="1.5"
				stroke="currentColor"
				class="w-6 h-6"
			>
				<path
					stroke-linecap="round"
					stroke-linejoin="round"
					d="M5.25 7.5A2.25 2.25 0 017.5 5.25h9a2.25 2.25 0 012.25 2.25v9a2.25 2.25 0 01-2.25 2.25h-9a2.25 2.25 0 01-2.25-2.25v-9z"
				/>
			</svg>
			<Tooltip>Stop Execution of the current pipeline</Tooltip>
		</Button>
	</div>
</div>
