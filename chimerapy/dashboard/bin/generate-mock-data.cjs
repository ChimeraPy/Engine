#! /usr/bin/env node

const { faker } = require('@faker-js/faker');
const { v4: uuidv4 } = require('uuid');

/*
 * @typedef {import('../src/lib/models').Manager} Manager
 * @typedef {import('../src/lib/models').Worker} Worker
 * @typedef {import('../src/lib/models').Node} Node
 */

/**
 * Generate test data for the dashboard
 * @param {number} numWorkers - The number of workers to generate
 * @param {number[]} nodesRange - [min, max] The number of nodes to generate for each worker
 * @returns {Manager} - The generated network
 */
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

/**
 * Generate a list of workers
 * @param {number} numWorkers - The number of workers to generate
 * @param {number[]} nodesRange - [min, max] The number of nodes to generate for each worker
 * @returns {Worker[]} - The generated workers
 */
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

/**
 * Randomly sample an element from an array
 * @template T
 * @param {T[]} array - The array to sample from
 * @returns {T} - A random element from the array
 */
function sample(array) {
	return array[Math.floor(Math.random() * array.length)];
}

/**
 * Generate a range of numbers
 * @param {number} start
 * @param end
 * @returns {number[]}
 */
function range(start, end) {
	return Array.from({ length: end - start }, (_, i) => i);
}

if (require.main === module) {
	console.log(JSON.stringify(generateNetwork(100, [0, 10]), null, 2));
}
