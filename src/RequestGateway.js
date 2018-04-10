"use strict";
const { parse: parseUrl } = require("url");
const { validate } = require("./validate");

class RequestGateway {

	async onRequest(request, response) {

		const ok = data => {

			respond(
				"ok",
				data
			);
		};

		const fault = (fault, data) => {

			respond(
				fault,
				data
			);
		};

		const respond = (code, data) => {

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

		// this fields
		const {
			log,
			api
		} = this;

		// request fields
		const {
			method,
			url,
			headers: {
				"authorization": authorization,
				"content-type": contentType
			},
			rawHeaders
		} = request;

		// handler fields
		let authorize;
		let cors;

		log.trace(
			"%s %s %j",
			method,
			url,
			rawHeaders
		);

		const table = api[
			method
		];

		if (table === undefined) {

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

			fault(
				"invalid-request"
			);

			return;
		}

		// read handler fields
		authorize = handler.authorize;
		cors = handler.cors;

		let token;
		if (authorize) {

			if (authorization === undefined) {

				fault(
					"not-authorized"
				);

				return;
			}

			try {
				token = await this.authorizationService.extract(
					authorization
				);
			}
			catch (error) {

				switch (error.message) {
					case "invalid-token":
						break;

					default:

						log.error(
							error.message
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

		switch (method) {

			case "POST": {

				let body;

				try {
					body = await new Promise((resolve, reject) => {

						const chunks = [];

						request.on("data", chunk => {

							chunks.push(
								chunk
							);
						});

						request.on("end", () => {

							if (0 < chunks.length) {

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

									reject(error);
									return;
								}

								resolve(body);
							}
							else {
								resolve();
							}
						});
					});
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

		// validate request

		if (handler.request === undefined) {
			// ok
		}
		else {

			const errors = validate(
				handler.request,
				request.body,
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

		let claims;
		if (authorize) {

			try {
				claims = await this.authorizationService.authorize(
					token,
					handler.action,
					request.body || {}
				);
			}
			catch (error) {

				switch (error.message) {
					case "not-authorized":
						break;

					default:
						log.error(
							error.message
						);

						break;
				}

				fault(
					"not-authorized"
				);

				return;
			}
		}

		// invoke handler

		let data;
		try {
			data = await handler.handle(
				request,
				response
			);
		}
		catch (error) {

			log.warn(
				error
			);

			if (response.finished) {
				// ok
			}
			else if (response.headersSent) {
				response.end();
			}
			else {

				let code;
				const faults = handler.faults;
				if (faults === undefined) {
					code = "internal-error";
				}
				else {
					const fault = faults[error.message];
					if (fault === undefined) {
						code = "internal-error";
					}
					else if (fault === null) {
						code = error.message;
					}
					else {
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

	async drain() {

		switch (this.state) {

			case null:
				break;

			case "draining":
				break;

			default:
				throw new Error();
		}
	}
}

RequestGateway.prototype.log = null;
RequestGateway.prototype.api = null;
RequestGateway.prototype.authorizationService = null;
RequestGateway.prototype.state = null;

module.exports = {
	RequestGateway
};
