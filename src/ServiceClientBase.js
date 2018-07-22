"use strict";
const http = require("http");
const https = require("https");
const { parse: parseUrl } = require("url");

const {
	hrtime
} = process;

class ServiceClientBase {

	post(endpoint, requestContent) {

		return new Promise((resolve, reject) => {

			const time = hrtime();

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

			options.headers = {};

			if (this.authorization === null) {
				// ok
			}
			else {
				options.headers["Authorization"] = this.authorization;
			}

			if (payload === undefined) {
				// ok
			}
			else {

				options.headers["Content-Type"] = "application/json; charset=utf-8";
				options.headers["Content-Length"] = `${payload.length}`;
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

							reject(
								error
							);

							return;
						}
					}

					const statusCode = response.statusCode;

					switch (statusCode) {

						case 200: {

							if (responseContent === undefined) {
								reject(
									new Error("service-client::empty-response")
								);
							}
							else if (responseContent.code === "ok") {

								const [s, ns] = hrtime(time);
								const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

								this.log.trace(
									"%s %s",
									url,
									elapsed
								);

								if (this.raw === true) {
									resolve(
										responseContent
									);
								}
								else {
									resolve(
										responseContent.data
									);
								}
							}
							else {

								const [s, ns] = hrtime(time);
								const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

								this.log.warn(
									"%s %s %s",
									url,
									responseContent.code,
									elapsed
								);

								const error = new Error(responseContent.code);
								error.data = responseContent.data;

								reject(
									error
								);
							}

							break;
						}

						default:

							reject(
								new Error(`http-${statusCode}`)
							);
							break;
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
ServiceClientBase.prototype.authorization = null;
ServiceClientBase.prototype.raw = false;

class ServiceClientBase2 {

	post(endpoint, principalId, requestContent) {

		return new Promise((resolve, reject) => {

			const time = hrtime();

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

			options.headers = {};

			if (principalId === undefined) {
				// ok
			}
			else {
				options.headers["x-fiyuu-principal"] = principalId;
			}

			// if (this.authorization === null) {
			// 	// ok
			// }
			// else {
			// 	options.headers["Authorization"] = this.authorization;
			// }

			if (payload === undefined) {
				// ok
			}
			else {

				options.headers["Content-Type"] = "application/json; charset=utf-8";
				options.headers["Content-Length"] = `${payload.length}`;
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

							reject(
								error
							);

							return;
						}
					}

					const statusCode = response.statusCode;

					switch (statusCode) {

						case 200: {

							if (responseContent === undefined) {
								reject(
									new Error("service-client::empty-response")
								);
							}
							else if (responseContent.code === "ok") {

								const [s, ns] = hrtime(time);
								const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

								this.log.trace(
									"%s %s",
									url,
									elapsed
								);

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
						}

						default:

							reject(
								new Error(`http-${statusCode}`)
							);
							break;
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

		class ServiceClient extends ServiceClientBase2 { }

		for (const key in methods) {

			ServiceClient.prototype[key] = function (headers, request) {

				const {
					principalId
				} = headers;

				return this.post(
					methods[key],
					principalId,
					request
				);
			}
		}

		return ServiceClient;
	}
}

ServiceClientBase2.prototype.log = null;
ServiceClientBase2.prototype.baseUrl = null;

module.exports = {
	ServiceClientBase,
	ServiceClientBase2
};
