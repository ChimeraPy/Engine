class Api {
	url: string;

	constructor(url: string) {
		this.url = url;
	}
}

export class MockApi extends Api {
	constructor(url: string) {
		super(url);
	}

	async getNetworkMap(): Promise<App.Manager> {
		return (await (await fetch('/mocks/networkMap.json')).json()) as App.Manager;
	}
}
