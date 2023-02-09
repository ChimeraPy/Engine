import { describe, it, expect } from 'vitest';

import type { Node } from '../../lib/models';
import { networkEntityDetails } from '../../lib/utils';

describe('utils', () => {
	it('should return proper object description for a node', () => {
		const node: Node = {
			id: 'Some Random Id',
			ip: 'http://locahost',
			port: 8001,
			name: 'My Node',
			data_type: 'Video',
			running: false,
			dashboard_component: null
		};

		const detailsString = networkEntityDetails(node, 0, 'Node');

		expect(detailsString).toBe(`Node #1 name = ${node.name} @ ${node.ip}:${node.port}`); // Follow Jest
	});
});
