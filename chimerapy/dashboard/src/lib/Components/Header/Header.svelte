<script lang="ts">
	import { page } from '$app/stores';
	import { Navbar, NavBrand, NavLi, NavUl, NavHamburger } from 'flowbite-svelte';

	export let sections: { string: string }[], logo;
</script>

<Navbar let:hidden let:toggle>
	<NavBrand href="/">
		<img src={logo} class="mr-3 h-6 sm:h-9" alt="ChimeraPy" />
	</NavBrand>
	<NavHamburger on:click={toggle} />
	<NavUl {hidden}>
		{#each sections as section, i}
			<NavLi
				href={section.slug}
				active={$page.url.pathname === section.slug}
				class="hover:text-blue-600">{section.title}</NavLi
			>
		{/each}
	</NavUl>
</Navbar>
