import * as zmq from 'jszmq';
import { awaitableDelay } from '../utils';

/**
 * ZMQSubscriber is a wrapper around the jszmq library for subscribing to a ZMQ publisher.
 * An example use case is subscribing to the ZMQ publisher in the ChimeraPy server to receive Logs.
 *  @param {string} publisherIp - The IP address of the ZMQ publisher.
 *  @param {number} targetPort - The port of the ZMQ publisher.
 *  @param {string} subscriptionTopic - The topic to subscribe to.
 *  @param {number} reconnectTimeout - The timeout in milliseconds to wait before attempting to reconnect.
 *  @param {number} retryCount - The number of times to retry connecting to the publisher before giving up.
 */

export class ZMQSubscriber {
	publisherIp: string;
	targetPort: number;
	subscriptionTopic: string;
	reconnectTimeout: number;
	retryCount: number;
	socket: zmq.Sub;
	protocol: 'ws' | 'wss';
	connectionAttempts: number = 0;

	constructor(
		publisherIp: string,
		targetPort: number,
		subscriptionTopic: string,
		reconnectTimeout: number,
		retryCount: number = 10,
		secure: boolean = false
	) {
		this.publisherIp = publisherIp;
		this.targetPort = targetPort;
		this.subscriptionTopic = subscriptionTopic;
		this.reconnectTimeout = reconnectTimeout;
		this.socket = new zmq.Sub();
		this.retryCount = retryCount;
		this.protocol = secure ? 'wss' : 'ws';
	}

	get publisherUrl(): string {
		return `${this.protocol}://${this.publisherIp}:${this.targetPort}`;
	}

	subscribe(callable: (data: string) => void, callableError: (e: Error) => void): () => void {
		this.socket.connect(this.publisherUrl);
		this.socket.subscribe(this.subscriptionTopic);
		this.socket.on('message', (topic: string, message: string) => {
			callable(message.toString());
		});

		const disconnect = () => {
			this.socket.disconnect(this.publisherUrl);
		};

		this.socket.on('error', async (e: Error) => {
			disconnect();
			if (this.connectionAttempts < this.retryCount) {
				this.connectionAttempts += 1;
				await awaitableDelay(this.reconnectTimeout);
				this.subscribe(callable, callableError);
			} else {
				console.error(`Failed to connect to ZMQ publisher after ${this.retryCount} attempts.`);
			}
		});

		return disconnect;
	}
}
