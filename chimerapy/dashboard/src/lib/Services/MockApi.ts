// ToDo
import { v4 as uuidv4 } from 'uuid';
import { faker } from '@faker-js/faker';

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

	async getNetwork(): Promise<App.Manager> {
		return this._generateNetwork(10, [0, 5]);
	}

	_generateNetwork(numWorkers: number, nodesRange: number[]): App.Manager {
		return {
			ip: faker.internet.ip(),
			port: faker.datatype.number({
				min: 8000,
				max: 20000
			}),
			workers: this._initializeWorkers(numWorkers, nodesRange)
		};
	}

	_initializeWorkers(numWorkers: number, nodesRange: number[]) {
		function range(start: number, end: number): number[] {
			return Array.from({ length: end - start }, (_, i) => i);
		}

		return range(0, numWorkers).map((idx) => {
			return {
				name: faker.lorem.word(10),
				ip: faker.internet.ip(),
				port: faker.datatype.number({
					min: 8000,
					max: 20000
				}),
				id: uuidv4(),
				nodes: range(0, this.sample(range(nodesRange[0], nodesRange[1]))).map((idx) => {
					return {
						id: uuidv4(),
						ip: faker.internet.ip(),
						port: faker.datatype.number({
							min: 8000,
							max: 20000
						}),
						name: faker.lorem.word(10),
						running: this.sample([true, false]),
						dashboard_component: null,
						data_type: this.sample(['Video', 'Series', 'Audio'])
					};
				})
			};
		});
	}

	sample<T>(array: Array<T>): T {
		return array[Math.floor(Math.random() * array.length)];
	}
}
