"use strict";
const fs = require("fs");
const { ServiceClientBase, ServiceClientBase2 } = require("./ServiceClientBase");
const b64u = require("./b64u");

const RequestServiceClient = ServiceClientBase.create({
	beginRequest: "/begin-request",
	completeRequest: "/complete-request"
});

const AccessServiceClient = ServiceClientBase2.create({
	createAccount: "/create-account",
	getAccount: "/get-account",
	removeAccount: "/remove-account",
	createUserPool: "/create-user-pool",
	removeUserPool: "/remove-user-pool",
	createGroup: "/create-group",
	createUser: "/create-user",
	authorizeToken: "/authorize-token",
	authorizeAPIKey: "/authorize-api-key"
});

const ProductServiceClient = ServiceClientBase2.create({
	createBrand: "/create-brand",
	removeBrand: "/remove-brand",
	removeBrands: "/remove-brands",
	getBrand: "/get-brand",
	queryBrandsByAccount: "/query-brands-by-account",

	createProductGroup: "/create-product-group",
	removeProductGroup: "/remove-product-group",
	removeProductGroups: "/remove-product-groups",
	queryProductGroupsByBrand: "/query-product-groups-by-brand",
	getProductGroup: "/get-product-group",

	createProduct: "/create-product",
	updateProduct: "/update-product",
	removeProduct: "/remove-product",
	removeProducts: "/remove-products",
	getProduct: "/get-product",
	getProductModifiers: "/get-product-modifiers",
	queryProductsByBrand: "/query-products-by-brand",
	queryProductsByProductGroup: "/query-products-by-product-group",

	createRestaurant: "/create-restaurant",
	removeRestaurant: "/remove-restaurant",
	removeRestaurants: "/remove-restaurants",
	getRestaurant: "/get-restaurant",
	queryRestaurantsByBrand: "/query-restaurants-by-brand",
});

class AuthorizationService {

	async extract(request) {

		const authorizationHeader = request.headers["authorization"];
		if (authorizationHeader === undefined) {
			throw new Error("invalid-token");
		}

		const authorizationMatch = authorizationHeader.match(/^(jwt|key) ([A-Za-z0-9\-_.]+)$/);
		if (authorizationMatch === null) {

			console.log(1);
			throw new Error("invalid-token");
		}

		const type = authorizationMatch[1];
		const token = authorizationMatch[2];

		console.log(type, token);

		switch (type) {

			case "jwt":

				const { verified } = this.jwtService.decode({
					token,
					publicKeys: this.publicKeys
				});

				if (verified === true) {
					// ok
				}
				else {

					throw new Error("invalid-token");
				}

				return {
					type,
					token
				};

			case "key": {

				const keyBuffer = b64u.toBuffer(token);

				return {
					type,
					token
				};
			}

			default:
				throw new Error("invalid-token");
		}
	}

	async authorize(request, authorizationInfo, action, resource) {

		const {
			type,
			token
		} = authorizationInfo;

		switch (type) {

			case "jwt": {

				try {
					return await this.accessService.authorizeToken({ principalId: this.serviceId }, {
						token,
						serviceId: this.serviceId,
						action,
						resource
					});
				}
				catch (error) {

					switch (error.message) {

						case "access::service-not-found":
						case "access::not-authorized":
							throw new Error("not-authorized");

						default:
							throw error;
					}
				}

				break;
			}

			case "key": {

				try {

					return await this.accessService.authorizeAPIKey({ principalId: this.serviceId }, {
						key: token,
						serviceId: this.serviceId,
						action,
						resource
					});
				}
				catch (error) {

					switch (error.message) {

						case "access::service-not-found":
						case "access::not-authorized":
							throw new Error("not-authorized");

						default:
							throw error;
					}
				}

				break;
			}

			default:
				throw new Error("not-authorized");
		}
	}
}

AuthorizationService.prototype.serviceId = null;
AuthorizationService.prototype.jwtService = null;
AuthorizationService.prototype.accessService = null;
AuthorizationService.prototype.publicKeys = null;

function startHttpServer(log, server, port) {

	return new Promise((resolve, reject) => {

		server.once("listening", () => {
			log.info("listening @ %j", server.address());
			resolve();
		});

		const iport = Number.parseInt(port);

		if (isNaN(iport)) {

			const socket = `/tmp/${port}.sock`;

			log.trace(
				"unlink %j...",
				socket
			);

			fs.unlink(socket, error => {

				if (error) {

					switch (error.code) {
						case "ENOENT":
							break;

						default:
							reject(error);
							return;
					}
				}

				server.listen(
					socket
				);
			});
		} else {

			server.listen(
				iport
			);
		}
	});
}

function stopHttpServer(log, server, port) {

	return new Promise((resolve, reject) => {

		server.once("close", () => {

			const iport = Number.parseInt(port);

			if (isNaN(iport)) {

				const socket = `/tmp/${port}.sock`;

				log.trace(
					"unlink %j...",
					socket
				);

				fs.unlink(socket, error => {

					if (error) {

						switch (error.code) {

							case "ENOENT":
								break;

							default:
								log.warn(error);
								reject(error);
								break;
						}
					}
					else {
						resolve();
					}
				});
			}
		});

		server.close();
	});

}

module.exports = {
	RequestServiceClient,
	AccessServiceClient,
	ProductServiceClient,
	AuthorizationService,
	startHttpServer,
	stopHttpServer
};
