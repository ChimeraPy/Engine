<script lang="ts">
	import { Navbar, NavBrand, NavLi, NavUl, NavHamburger, DarkMode } from 'flowbite-svelte';

	export let sections: { string: string }[];
	const logo = '/ChimeraPy.png';
</script>

<div class="mx-auto">
	<Navbar
		let:hidden
		let:toggle
		navClass="px-2 py-0.5 fixed w-full mx-auto z-20 top-0 left-0 border-b"
	>
		<NavBrand href="/">
			<img src={logo} class="mr-3 h-6 sm:h-9" alt="ChimeraPy" />
		</NavBrand>
		<NavHamburger on:click={toggle} />
		<NavUl {hidden}>
			{#each sections as section, i}
				<NavLi href={section.slug} active={false}>{section.title}</NavLi>
			{/each}
		</NavUl>
		<DarkMode class="text-lg" />
	</Navbar>
</div>
