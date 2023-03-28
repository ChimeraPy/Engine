import type { Worker, Node } from './models';

export function networkEntityDetails(
	entity: Worker | Node,
	index: number,
	prefix = 'Worker'
): string {
	const details = [
		prefix,
		`#${index + 1}`,
		'name =',
		entity.name,
		'@',
		`${entity.ip}:${entity.port}`
	];
	return details.join(' ');
}

export function awaitableDelay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
