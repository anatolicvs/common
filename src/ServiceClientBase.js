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
				case "ipc:":
					lib = http;
					break;

				case "https:":
				case "ipcs:":
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
				method: "POST",
				path: urlInfo.path
			};

			switch (urlInfo.protocol) {
				case "ipc:":
					options.protocol = "http:";
					options.socketPath = `/tmp/${urlInfo.hostname}.sock`;
					break;

				case "ipcs:":
					options.protocol = "https:";
					options.socketPath = `/tmp/${urlInfo.hostname}.sock`;
					break;

				case "http:":
				case "https:":
					options.protocol = urlInfo.protocol;
					options.hostname = urlInfo.hostname;
					options.port = urlInfo.port;
					break;

				default:
					throw new Error();
			}

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

								const error = new Error(responseContent.code);
								error.data = responseContent.data;
								reject(
									error
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
				request.end();
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
