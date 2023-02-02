<script lang="ts">
    import {Footer, FooterCopyright, FooterLinkGroup, FooterLink} from 'flowbite-svelte';

    export let sections;

    // ToDo: make Footer sticky
</script>

<Footer class="my-10">
    <div class="container mx-auto">
        <FooterCopyright href="https://wp0.vanderbilt.edu/oele/" by="oele-vanderbilt" year={2023}/>
    </div>
</Footer>
