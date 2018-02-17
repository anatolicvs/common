"use strict";
const { createServer, ServerResponse } = require("http");
const { parse: parseUrl } = require("url");
const { validate } = require("./validate");

ServerResponse.prototype.json = function (value) {

	const payload = Buffer.from(JSON.stringify(
		value
	));

	this.statusCode = 200;
	this.setHeader("Content-Type", "application/json; charset=utf-8");
	this.setHeader("Content-Length", `${payload.length}`);
	this.end(payload);
}

ServerResponse.prototype.respond = function (code, data) {

	this.json({ code, data });
}

ServerResponse.prototype.ok = function (data) {

	this.respond("ok", data);
}

ServerResponse.prototype.fault = function (fault, data) {

	this.respond(fault, data);
}

function createHttpServer({ api, log }) {

	return createServer(async (request, response) => {

		const method = request.method;
		const url = request.url;

		log.trace("%j %j %j", method, url, request.rawHeaders);

		const table = api[
			method
		];

		if (table === undefined) {

			response.fault("invalid-request");
			return;
		}

		const { pathname } = parseUrl(
			url
		);

		const handler = table[
			pathname
		];

		if (handler === undefined) {

			response.fault("invalid-request");
			return;
		}

		// read any content

		switch (method) {

			case "POST": {

				await new Promise((resolve, reject) => {

					const chunks = [];

					request.on("data", chunk => {

						chunks.push(
							chunk
						);
					});

					request.on("end", () => {

						if (0 < chunks.length) {
							request.body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
						}

						resolve();
					});
				});

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
					log.warn(error);
				}

				response.fault("invalid-request", errors);
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
				const faults = request.faults;
				if (faults === undefined) {
					code = "internal-error";
				}
				else {
					const fault = faults[error.message];
					if (fault === undefined) {
						code = "internal-error";
					}
					else {
						code = fault;
					}
				}

				response.fault(code);
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
			response.ok(data);
		}
	});
}

module.exports = {
	createHttpServer
}