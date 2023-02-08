export function objectDetails(worker: App.Worker, workerIndex: number, prefix = 'Worker'): string {
	const details = [
		prefix,
		`#${workerIndex + 1}`,
		'name = ',
		worker.name,
		' ',
		`${worker.ip}@${worker.port}`
	];
	return details.join(' ');
}
