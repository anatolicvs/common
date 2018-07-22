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

		tools.getCode(requestId, "requestId");
		tools.getCode(code, "code");

		const request = await this.db.getRequest(
			requestId
		);

		if (request === undefined) {

			this.log.warn(
				"request (%j) is not found.",
				requestId
			);

			throw new Error("request::request-not-found");
		}

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

			const responseContentJson = JSON.stringify(
				content
			);

			request.response = responseContentJson;
		}

		request.state = 1;
		request.updatedAt = tools.ts();

		await this.db.updateRequest(
			request
		);
	}

	async getRequest(principalId, requestId) {

		const request = await this.db.getRequest(
			requestId
		);

		if (request === undefined) {

			this.log.warn(
				"request (%j) is not found.",
				requestId
			);

			throw new Error("request::request-not-found");
		}

		if (request.principalId === principalId) {
			// ok
		}
		else {

			this.log.warn(
				"request[%j].principalId (%j) is not equal to principalId (%j).",
				request.id,
				request.principalId,
				principalId
			);

			throw new Error("request::request-not-found");
		}

		const {
			createdAt,
			serviceId,
			action,
			state,
			code,
			response: responseContentJson
		} = request;

		switch (state) {
			case 0:
				throw new Error("request::request-not-complete");

			case 1:
				// ok
				break;

			default:
				throw new Error("request::request-corrupted");
		}

		const result = {
			code
		};

		if (responseContentJson === undefined) {
			// ok
		}
		else {
			result.data = JSON.parse(
				responseContentJson
			)
		}

		return result;
	}
}

RequestService.prototype.log = null;
RequestService.prototype.db = null;
RequestService.prototype.newid = null;

module.exports = {
	RequestService
};
