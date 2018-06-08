"use strict";
const { parse: parseUrl } = require("url");
const { validate } = require("./validate");

class RequestGateway {

	async onRequest(request, response) {

		function ok(data) {

			respond(
				"ok",
				data
			);
		};

		function fault(fault, data) {

			respond(
				fault,
				data
			);
		};

		function respond(code, data) {

			const payload = Buffer.from(JSON.stringify({
				code,
				data
			}));

			response.statusCode = 200;

			if (cors) {

				response.setHeader(
					"Access-Control-Allow-Origin",
					"*"
				);
			}

			response.setHeader(
				"Content-Type",
				"application/json; charset=utf-8"
			);

			response.setHeader(
				"Content-Length",
				`${payload.length}`
			);

			response.end(
				payload
			);
		}

		function readRequestContent() {

			return new Promise((resolve, reject) => {

				const chunks = [];

				request.on("data", chunk => {

					chunks.push(
						chunk
					);
				});

				request.once("end", () => {

					if (0 < chunks.length) {
					}
					else {
						resolve();
						return;
					}

					let body;
					try {

						switch (contentType) {

							case "application/x-www-form-urlencoded": {

								const pairs = Buffer.concat(chunks).toString("utf8").split('&');

								body = {};

								for (let pair of pairs) {

									pair = pair.replace(/\+/g, "%20");

									const index = pair.indexOf("=");
									let key;
									let value;

									if (index < 0) {
										key = decodeURIComponent(pair);
										value = "";
									}
									else {
										key = decodeURIComponent(pair.substr(0, index));
										value = decodeURIComponent(pair.substr(index + 1));
									}

									body[key] = value;
								}

								break;
							}

							default:
								body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
								break;
						}
					}
					catch (error) {

						reject(
							error
						);

						return;
					}

					resolve(
						body
					);
				});
			});
		}

		// this fields
		const {
			log,
			api,
			authorizationService,
			//requestService,
			instances
		} = this;

		// request fields
		const {
			method: requestMethod,
			url,
			headers: {
				"content-type": contentType
			},
			rawHeaders
		} = request;

		// handler fields
		let authorize;
		let cors;
		let requestSchema;

		const table = api[
			requestMethod
		];

		if (table === undefined) {

			log.warn(
				"%s %s %j",
				requestMethod,
				url,
				rawHeaders
			);

			fault(
				"invalid-request"
			);

			return;
		}

		const {
			pathname
		} = parseUrl(
			url
		);

		const handler = table[
			pathname
		];

		if (handler === undefined) {

			log.warn(
				"%s %s %j",
				requestMethod,
				url,
				rawHeaders
			);

			fault(
				"invalid-request"
			);

			return;
		}

		// read handler fields
		authorize = handler.authorize;
		cors = handler.cors;
		requestSchema = handler.request;

		let authorizationInfo;
		if (authorize) {

			try {
				authorizationInfo = await authorizationService.extract(
					request
				);
			}
			catch (error) {

				switch (error.message) {
					case "invalid-token":
						break;

					default:

						log.error(
							error
						);

						break;
				}

				fault(
					"not-authorized"
				);

				return;
			}
		}

		// read any content

		let body;

		switch (requestMethod) {

			case "POST": {

				try {
					body = await readRequestContent();
				}
				catch (error) {

					log.warn(
						error
					);

					fault(
						"invalid-request"
					);
					return;
				}

				request.body = body;
				break;
			}
		}

		log.debug(
			"body: %j",
			body
		);

		// validate request

		if (requestSchema === undefined) {
			// ok
		}
		else {

			const errors = validate(
				requestSchema,
				body,
				"body"
			);

			if (errors === undefined) {
				// ok
			}
			else {

				for (const error of errors) {
					log.warn(
						error
					);
				}

				fault(
					"invalid-request",
					errors
				);

				return;
			}
		}

		// authorize

		let principalId;
		if (authorize) {

			let authorizationResult;

			try {
				authorizationResult = await authorizationService.authorize(
					request,
					authorizationInfo,
					handler.serviceId,
					handler.action,
					body || {}
				);
			}
			catch (error) {

				switch (error.message) {
					case "not-authorized":
						break;

					default:
						log.error(
							error
						);

						break;
				}

				fault(
					"not-authorized"
				);

				return;
			}

			principalId = authorizationResult.principalId;
		}

		// invoke

		let data;
		try {

			if (handler.handle2 !== undefined) {

				data = await handler.handle2(
					{ principalId },
					body
				);
			}
			else if (handler.handle !== undefined) {

				data = await handler.handle(
					request,
					response
				);
			}
			else {

				const instanceName = handler.instance;
				const instance = instances[instanceName];

				const methodName = handler.method;
				//const async = handler.async;

				const invocationHeaders = {};

				data = await instance[methodName](
					{ principalId },
					body
				);
			}
		}
		catch (error) {

			if (response.finished) {

				log.warn(
					error
				);
			}
			else if (response.headersSent) {

				response.end();

				log.warn(
					error
				);
			}
			else {

				let code;
				const faults = handler.faults;
				if (faults === undefined) {

					log.warn(
						error
					);

					code = "internal-error";
				}
				else {

					const fault = faults[error.message];
					if (fault === undefined) {

						log.warn(
							error
						);

						code = "internal-error";
					}
					else if (fault === null) {

						log.warn(
							error.message
						);

						code = error.message;
					}
					else {

						log.warn(
							fault
						);

						code = fault;
					}
				}

				fault(code);
			}

			return;
		}

		// check response

		if (response.finished) {
			// ok
		}
		else if (response.headersSent) {
			response.end();
		}
		else {

			if (handler.response === undefined) {
				ok(data);
			}
			else {

				const errors = validate(
					handler.response,
					data,
					"response"
				);

				if (errors === undefined) {
					ok(data);
				}
				else {
					fault("internal-error");
				}
			}
		}
	}
}

RequestGateway.prototype.log = null;
RequestGateway.prototype.api = null;
RequestGateway.prototype.authorizationService = null;
RequestGateway.prototype.instances = null;

module.exports = {
	RequestGateway
};
