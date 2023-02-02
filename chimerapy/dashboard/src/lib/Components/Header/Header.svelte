<script lang="ts">
   import { Navbar, NavBrand, NavLi, NavUl, NavHamburger } from 'flowbite-svelte';

   export let sections: {string: string}[];

</script>


<Navbar let:hidden let:toggle>
  <NavBrand href="/">
    <img
      src="https://chimerapy.readthedocs.io/en/latest/_images/chimerapy_logo_with_name_theme_blue.png"
      class="mr-3 h-6 sm:h-9"
      alt="ChimeraPy"
    />
  </NavBrand>
  <NavHamburger on:click={toggle} />
  <NavUl {hidden}>
      {#each sections as section, i}
          <NavLi href="{section.slug}" active={false}>{section.title}</NavLi>
      {/each}
  </NavUl>
</Navbar>
