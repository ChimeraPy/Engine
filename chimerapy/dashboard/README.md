# ChimeraPy-Dashboard

The front-end dashboard application for [`ChimeraPy`](https://github.com/oele-isis-vanderbit/ChimeraPy), written using [`svelte-kit`](https://kit.svelte.dev).

The dashboard allows any ChimeraPy deployment to monitor the cluster as well as orchestrate MMLA pipeline accordingly.

:warning: This project is in very early stages of development and should be considered unstable to use currently.

## Installation

Node `v16` or later is required. Tested and developed with `v16`.

Use the following commands to install.

```bash
git clone git@github.com:oele-isis-vanderbilt/ChimeraPy.git

cd ChimeraPy/chimerapy/dashboard
npm install

```

## Developing

Once you've created a project and installed dependencies with `npm install` (or `pnpm install` or `yarn`), start a development server:

The following commands will spin up the test development server at http://localhost:5173.

```bash
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

## Development RoadMap

This is something on the works. Follow [RoadMap.md](./dev-docs/RoadMap.md) for a quick and dirty version.
