"use strict";
const { createServer, ServerResponse } = require("http");
const { parse: parseUrl } = require("url");

ServerResponse.prototype.json = function (value) {

	const payload = Buffer.from(JSON.stringify(
		value
	));

	this.statusCode = 200;
	this.setHeader("Content-Type", "application/json; charset=utf-8");
	this.setHeader("Content-Length", `${payload.length}`);
	this.end(payload);
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
			response.statusCode = 400;
			response.setHeader("Connection", "close");
			response.end();
			return;
		}

		const { pathname } = parseUrl(
			url
		);

		const handler = table[
			pathname
		];

		if (handler === undefined) {
			response.statusCode = 400;
			response.setHeader("Connection", "close");
			response.end();
			return;
		}

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

		switch (typeof handler) {

			case "function": {

				try {
					await handler(
						request,
						response
					);
				}
				catch (error) {

					log.warn(error);

					if (response.finished) {
						// ok
					}
					else if (response.headersSent) {
						response.end();
					}
					else {
						response.statusCode = 500;
						response.end();
					}

					return;
				}

				if (response.finished) {
					// ok
				}
				else if (response.headersSent) {
					response.end();
				}
				else {
					response.statusCode = 500;
					response.end();
				}

				break;
			}

			case "object": {

				try {
					await handler.handle(
						request,
						response
					);
				}
				catch (error) {

					log.warn(error);

					if (response.finished) {
						// ok
					}
					else if (response.headersSent) {
						response.end();
					}
					else {
						response.statusCode = 500;
						response.end();
					}

					return;
				}

				if (response.finished) {
					// ok
				}
				else if (response.headersSent) {
					response.end();
				}
				else {
					response.statusCode = 500;
					response.end();
				}

				break;
			}

			default: {
				response.statusCode = 500;
				response.end();
			}
		}
	});
}

module.exports = {
	createHttpServer
}