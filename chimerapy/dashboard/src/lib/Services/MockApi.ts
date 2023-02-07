// ToDo
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

    getNetwork() {}
}