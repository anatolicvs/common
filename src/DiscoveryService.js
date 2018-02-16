"use strict";
const tools = require("./tools");

class DiscoveryService {

	get(id) {

		const service = this.registry[id];
		if (service === undefined) {

			throw new Error(
				tools.format(
					"service %j not found",
					id
				)
			);
		}

		return service;
	}
}

DiscoveryService.prototype.registry = null;

module.exports = {
	DiscoveryService
};
