"use strict";
const http = require("http");
const https = require("https");
const { parse: parseUrl } = require("url");

class ServiceClientBase {

	post(endpoint, requestContent) {

		return new Promise((resolve, reject) => {

			const url = `${this.baseUrl}${endpoint}`;
			const urlInfo = parseUrl(
				url
			);

			let lib;
			switch (urlInfo.protocol) {
				case "http:":
					lib = http;
					break;

				case "https:":
					lib = https;
					break;

				default:
					throw new Error();
			}

			let payload;
			if (requestContent === undefined) {
				// ok
			}
			else {
				payload = Buffer.from(
					JSON.stringify(
						requestContent
					),
					"utf8"
				);
			}

			const options = {
				protocol: urlInfo.protocol,
				hostname: urlInfo.hostname,
				port: urlInfo.port,
				method: "POST",
				path: urlInfo.path,
				headers: {
					"Content-Type": "application/json; charset=utf-8",
					"Content-Length": `${payload.length}`
				}
			};

			if (payload === undefined) {
				// ok
			}
			else {

				options.headers = {
					"Content-Type": "application/json; charset=utf-8",
					"Content-Length": `${payload.length}`
				};
			}

			const request = lib.request(options, response => {

				const chunks = [];

				response.on("data", chunk => {

					chunks.push(
						chunk
					);
				});

				response.on("error", error => {

					reject(
						error
					);
				});

				response.on("end", () => {

					let responseContent;

					if (0 < chunks.length) {
						try {
							responseContent = JSON.parse(
								Buffer.concat(chunks).toString("utf8")
							);
						}
						catch (error) {

							return reject(
								error
							);
						}
					}

					const statusCode = response.statusCode;

					switch (statusCode) {

						case 200:
							resolve(
								responseContent
							);
							break;

						case 400:
						case 500:
							reject(
								new Error(
									responseContent.code
								)
							);
							break;

						default:
							reject(
								new Error(`http-${statusCode}`)
							);
					}
				});
			})

			request.on("error", error => {
				reject(error);
			});

			if (payload === undefined) {
				// ok
			}
			else {
				request.end(
					payload
				);
			}
		});
	}

	static create(methods) {

		const result = class extends ServiceClientBase { }

		for (const key in methods) {

			result.prototype[key] = async function (request) {
				await this.post(
					methods[key],
					request
				);
			}

			return result;
		}
	}
}

ServiceClientBase.prototype.baseUrl = null;

module.exports = {
	ServiceClientBase
};
