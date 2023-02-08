#! /usr/bin/env node

const { faker } = require('@faker-js/faker');
const { v4: uuidv4 } = require('uuid');

function generateNetwork(numWorkers, nodesRange) {
	return {
		ip: faker.internet.ip(),
		port: faker.datatype.number({
			min: 8000,
			max: 20000
		}),
		workers: initializeWorkers(numWorkers, nodesRange)
	};
}

function initializeWorkers(numWorkers, nodesRange) {
	return range(0, numWorkers).map(() => {
		return {
			name: faker.lorem.word(10),
			ip: faker.internet.ip(),
			port: faker.datatype.number({
				min: 8000,
				max: 20000
			}),
			id: uuidv4(),
			nodes: range(0, sample(range(nodesRange[0], nodesRange[1]))).map(() => {
				return {
					id: uuidv4(),
					ip: faker.internet.ip(),
					port: faker.datatype.number({
						min: 8000,
						max: 20000
					}),
					name: faker.lorem.word(10),
					running: sample([true, false]),
					dashboard_component: null,
					data_type: sample(['Video', 'Series', 'Audio'])
				};
			})
		};
	});
}

function sample(array) {
	return array[Math.floor(Math.random() * array.length)];
}

function range(start, end) {
	return Array.from({ length: end - start }, (_, i) => i);
}

if (require.main === module) {
	console.log(JSON.stringify(generateNetwork(100, [0, 10]), null, 2));
}
