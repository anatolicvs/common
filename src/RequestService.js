"use strict";
const tools = require("./tools");

class RequestService {

	async createRequest(serviceId, action, accountId, principalId, content) {

		// {
		// 	"action": "create-brand",
		// 	"createdAt": 1527281173652,
		// 	"id": "9c1523f278189825c296244acdccd1a8",
		// 	"iv": 0,
		// 	"principalId": "16e3a270ef42de221eb248cb9f7c74f4",
		// 	"serviceId": "oms.product",
		// 	"state": 0
		//   }

		const requestId = this.newid();

		const request = {
			id: requestId,
			createdAt: tools.ts(),

			serviceId,
			action,

			accountId,
			principalId,

			state: 0
		};

		if (content === undefined) {
			// ok
		}
		else {
			request.request = JSON.stringify(content);
		}

		await this.db.createRequest(
			request
		);

		return requestId;
	}

	async completeRequest(requestId, code, content) {

		const request = await this.db.getRequest(
			requestId
		);

		if (request.state === 0) {
			// ok
		}
		else {
			throw new Error();
		}

		request.code = code;

		if (content === undefined) {
			// ok
		}
		else {
			request.response = JSON.stringify(
				content
			);
		}

		request.state = 1;

		await this.db.updateRequest(
			request
		);
	}
}

RequestService.prototype.log = null;
RequestService.prototype.db = null;
RequestService.prototype.newid = null;

module.exports = {
	RequestService
};
