"use strict";
const { createServer } = require("http");
const { parse: parseUrl } = require("url");

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
	});
}

module.exports = {
	createHttpServer
}