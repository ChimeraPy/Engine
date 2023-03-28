<script lang="ts">
	import { VirtualScroll } from 'svelte-virtual-scroll-list';
	import { Heading } from 'flowbite-svelte';

	export let logs: { id: number; text: string }[] = [],
		footerText = '',
		title = 'Logs';
	let isLogListHovered = false;
	let virtualScroll: VirtualScroll;

	export function isHovered() {
		return isLogListHovered;
	}

	export function scrollToBottom() {
		virtualScroll.scrollToBottom();
	}

	export function scrollToTop() {
		virtualScroll.scrollToTop();
	}

	export function addScrollClass() {}
</script>

<!--FixMe: Sometimes the scrollbar is not behaving correctly. This is from the underlying library.-->
<Heading tag="h3" class="bg-green-900 text-white p-2">{title}</Heading>
<div
	class="h-96 flex bg-gray-900"
	on:mouseover={() => (isLogListHovered = true)}
	on:mouseout={() => (isLogListHovered = false)}
>
	<VirtualScroll
		bind:this={virtualScroll}
		data={logs}
		key="id"
		containerClasses={['scrollbar-thin', 'scrollbar-thumb-gray-100', 'scrollbar-track-gray-700']}
		let:data
	>
		<div class="flex flex-row text-white h-100 w-100 px-2">
			<div class="mr-2">{data.id}</div>
			<div class="flex-1">{data.text}</div>
		</div>

		<div slot="footer" class="text-center">
			{footerText}
		</div>
	</VirtualScroll>
</div>
