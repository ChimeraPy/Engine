<script lang="ts">
	import { Img, P, Heading, Mark, Button } from 'flowbite-svelte';
</script>

<svelte:head>
	<title>ChimeraPy Dashboard</title>
</svelte:head>

<div id="homepage">
	<Img class="p-2 mt-2" src="/ChimeraPy.png" size="max-w-lg" alignment="mx-auto" alt="ChimeraPy" />

	<P class="mb-10 mt-10 text-justify" weight="normal" color="text-black dark:text-gray-400">
		ChimeraPy is a distributed computing framework for
		<Mark><b>multimodal data collection</b></Mark>
		,
		<Mark><b>processing</b></Mark>
		, and
		<Mark><b>feedback</b></Mark>
		. It's a real-time streaming tool that leverages from a distributed cluster to empower AI-driven
		applications.
	</P>
	<hr />
	<div class="grid md:grid-cols-1 grid-cols-1 md:gap-4 place-items-center my-10">
		<div class="text-center">
			<Heading tag="h1" class="mb-4" customSize="text-4xl font-extrabold  md:text-5xl lg:text-6xl"
				>Learn More
			</Heading>
			<P class="mb-6 text-lg lg:text-xl sm:px-16 xl:px-48 dark:text-gray-400"
				>Distributed P2P Networking for Multimodal Data Collection</P
			>
			<Button href="https://github.com/oele-isis-vanderbilt/ChimeraPy" target="_blank"
				>Go To Github!
				<svg
					class="ml-2 -mr-1 w-5 h-5"
					fill="currentColor"
					viewBox="0 0 20 20"
					xmlns="http://www.w3.org/2000/svg"
				>
					<path
						fill-rule="evenodd"
						d="M10.293 3.293a1 1 0 011.414 0l6 6a1 1 0 010 1.414l-6 6a1 1 0 01-1.414-1.414L14.586 11H3a1 1 0 110-2h11.586l-4.293-4.293a1 1 0 010-1.414z"
						clip-rule="evenodd"
					/>
				</svg>
			</Button>
		</div>
	</div>
	<hr />
	<div class="text-center my-10">
		<Heading tag="h2" customSize="text-4xl font-extrabold">Tutorial Videos</Heading>
	</div>

	<div class="grid md:grid-cols-2 grid-cols-1 md:gap-4 place-items-center my-10">
		<div>
			<iframe
				width="560"
				height="315"
				src="https://www.youtube.com/embed/dQw4w9WgXcQ"
				title="YouTube video player"
				frameborder="0"
				allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
				allowfullscreen
			/>
		</div>
		<div>
			<iframe
				width="560"
				height="315"
				src="https://www.youtube.com/embed/dQw4w9WgXcQ"
				title="YouTube video player"
				frameborder="0"
				allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
				allowfullscreen
			/>
		</div>
	</div>
	<hr />
</div>
