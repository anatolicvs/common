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

				this.log.trace("%j %j", response.statusCode, response.rawHeaders);

				const chunks = [];

				response.on("data", chunk => {

					chunks.push(
						chunk
					);
				});

				response.on("error", error => {

					this.log.warn(error);
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

							this.log.warn(error);
							return reject(
								error
							);
						}
					}

					const statusCode = response.statusCode;

					switch (statusCode) {

						case 200:
							if (responseContent === undefined) {
								reject(
									new Error("service-client::empty-response")
								);
							}
							else if (responseContent.code === "ok") {
								resolve(
									responseContent.data
								);
							}
							else {
								reject(
									new Error(responseContent.code)
								);
							}
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

		class ServiceClient extends ServiceClientBase { }

		for (const key in methods) {

			ServiceClient.prototype[key] = function (request) {
				return this.post(
					methods[key],
					request
				);
			}
		}

		return ServiceClient;
	}
}

ServiceClientBase.prototype.log = null;
ServiceClientBase.prototype.baseUrl = null;

module.exports = {
	ServiceClientBase
};
