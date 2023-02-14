import { describe, beforeAll, afterAll, expect, it } from 'vitest';
import { ZMQSubscriber } from '../../../lib/Services/ZMQSubscriber';
import { spawn } from 'child_process';
import * as path from 'path';

describe('ZMQSubscriber', () => {
	let spawnProcess: any;
	let zmqSubscriber: ZMQSubscriber;
	beforeAll(() => {
		spawnProcess = spawn('python', [
			'-u',
			path.join(__dirname, '..', '..', '..', '..', 'dummy_log_server.py')
		]);
		zmqSubscriber = new ZMQSubscriber('localhost', 10000, 'mock_logs', 10000, 10, false);
	});

	it('should subscribe to logs', async () => {
		const disconnect = zmqSubscriber.subscribe(
			(data: string) => {
				expect(data.toString()).toBeTypeOf('string');
				disconnect();
			},
			(e: Error) => {
				console.error(e);
			}
		);
	});

	afterAll(() => {
		spawnProcess.kill();
	});
});
