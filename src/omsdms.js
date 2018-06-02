"use strict";
const fs = require("fs");
const http = require("http");
// const aws = require("aws-sdk");

const tools = require("./tools");
const {
	SimpleLogService, StdoutAppender, RedisAppender, Log
} = require("./logging");

const {
	createRedisConnection
} = require("./createRedisConnection");

const {
	JWTService
} = require("./JWTService");

const {
	ServiceClientBase,
	ServiceClientBase2,
} = require("./ServiceClientBase");

const {
	RequestGateway,
} = require("./RequestGateway");

const b64u = require("./b64u");

const { validate } = require("./validate");

const {
	DataAccess
} = require("./DataAccess");
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

	extract(request) {

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

				this.log.trace(
					"extract key..."
				);

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

	async authorize(request, authorizationInfo, serviceId, action, resource) {

		const {
			type,
			token
		} = authorizationInfo;

		switch (type) {

			case "jwt": {

				try {
					return await this.accessService.authorizeToken({ principalId: serviceId }, {
						token,
						serviceId,
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

				this.log.trace(
					"authorize key..."
				);

				try {

					return await this.accessService.authorizeAPIKey({ principalId: serviceId }, {
						key: token,
						serviceId,
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

AuthorizationService.prototype.log = null;
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
								resolve();
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

function hostAPI({
	name,
	createDataAccess,
	createCacheRedis,
	configs,
	api,
	createService
}) {

	const env = process.env.FIYUU_ENV;
	if (env === undefined) {
		throw new Error("define FIYUU_ENV!");
	}

	const config = configs[env];
	if (config === undefined) {
		throw new Error("config not found.");
	}

	if (process.env.PORT) {
		config.port = process.env.PORT;
	}

	const simpleLogService = new SimpleLogService();

	const stdoutAppender = new StdoutAppender();
	simpleLogService.appenders.push(
		stdoutAppender
	);

	function createLog(category) {

		const log = new Log();
		log.service = simpleLogService;
		log.category = category;

		return log;
	}

	const log = createLog(`host`);

	log.trace(
		"create redis appender..."
	);

	const redisAppender = new RedisAppender();
	redisAppender.app = name;
	redisAppender.env = env;
	redisAppender.channel = "livelog";

	simpleLogService.appenders.push(
		redisAppender
	);

	//

	let ddb;
	let sqs;
	let da;

	if (createDataAccess === true) {

		log.trace(
			"require aws-sdk..."
		);

		const aws = require("aws-sdk");

		log.trace(
			"configure aws-sdk..."
		);

		aws.config.update({
			region: "eu-west-1"
		});

		ddb = new aws.DynamoDB.DocumentClient(
			config.ddbOptions
		);

		sqs = new aws.SQS(
			config.sqsOptions
		);

		da = new DataAccess();
		da.log = createLog("da");
		da.tableNamePrefix = config.tableNamePrefix;
		da.ddb = ddb;
	}

	//

	const jwtService = new JWTService();

	const accessServiceClient = new AccessServiceClient();
	accessServiceClient.log = createLog("access-service-client");
	accessServiceClient.baseUrl = config.accessServiceBaseUrl;

	const authorizationService = new AuthorizationService();
	authorizationService.log = createLog("authorization");
	authorizationService.jwtService = jwtService;
	authorizationService.accessService = accessServiceClient;
	authorizationService.publicKeys = {
		"key-1": "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtHJJqTPTTm8U56NFbwfo\nCqoIAwCSzvJn9tipY8klvGQENp2g1Drs600PSNiDrzOWBY/ahGFQixmbuBeHSO2P\nsdgdGs0ChKNBBC2Ow5GzSaDHC6OZbGDlPHvtnFkJL2WUm4ZcsO0wnllQaCq66loM\nVBXEAsY8fYdf+kNkmfa3lJ6ybJ1mJw7cryiupqZ/8Tl+N4MZruc4f7RlXfH4ogew\nvxIeGlbBqWgUV8K4nsLDvT348mWCnozPDZFc1Xhfj/8YpX2spfbuy/wr1nU+HYUS\n3K2dYgpMY+eo2nxJRoKQPg6Z+BrUaxY2mlq0QEHwKAo1cMGX+gtKWKeBn6ECOYrS\nzQIDAQAB\n-----END PUBLIC KEY-----"
	};


	const instances = createService({
		createLog,
		ddb,
		sqs,
		da
	});

	for (const instanceName in instances) {

		const instance = instances[instanceName];

		const instanceConfig = config[instanceName];
		if (instanceConfig === undefined) {
			continue;
		}

		for (const propertyName in instanceConfig) {

			const value = instanceConfig[
				propertyName
			];

			if (instance[propertyName] === undefined) {
				throw new Error();
			}

			instance[propertyName] = value;
		}
	}

	const httpapi = {

		GET: {

			"/": {
				handle(request, response) {
					response.statusCode = 200;
					response.end();
				}
			},

			"/health-check": {
				handle(request, response) {
					response.statusCode = 200;
					response.end();
				}
			}
		},
		POST: {
			...api.endpoints
		},
		OPTIONS: {
		}
	};

	for (const path in httpapi.POST) {

		httpapi.OPTIONS[path] = {
			handle: cors
		};
	}

	function cors(request, response) {

		response.statusCode = 200;
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "OPTIONS, POST");
		response.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");

		response.end();
	};

	const requestGateway = new RequestGateway();
	requestGateway.log = createLog("request-gateway");
	requestGateway.api = httpapi;
	requestGateway.authorizationService = authorizationService;
	requestGateway.instances = instances;

	const requestGatewayOnRequest = requestGateway.onRequest.bind(
		requestGateway
	);

	const server = http.createServer();

	let cacheRedis = null;
	let publishRedis = null;

	process.on("unhandledRejection", error => {
		log.error("unhandled rejection:", error);
	});

	let socketCount = 0;

	async function start() {

		if (createCacheRedis === true) {
			cacheRedis = createRedisConnection(
				config.redisOptions,
				createLog("cache-redis")
			);
		}

		publishRedis = createRedisConnection(
			config.redisOptions,
			createLog("publish-redis")
		);

		if (da === undefined) {
			// ok
		}
		else {
			if (createCacheRedis === true) {
				da.redis = cacheRedis;
			}
		}

		redisAppender.redis = publishRedis;
		// const aws = require("aws-sdk");

		// aws.config.update({
		// 	region: "eu-west-1"
		// });

		server.on("connection", socket => {

			socketCount++;

			socket.once("close", hadError => {

				socketCount--;
			});
		});

		server.on(
			"request",
			requestGatewayOnRequest
		);

		await startHttpServer(log, server, config.port);


		process.once("SIGTERM", async () => {

			try {

				log.trace("stop...");
				await stop();

				log.trace("exit...");
				process.exit(0);
			}
			catch (error) {

				console.log(error);
			}
		});

		// nodemon restart handler
		process.once("SIGUSR2", async () => {

			try {
				log.trace("stop...");
				await stop();

				log.trace("kill...");
				process.kill(process.pid, "SIGUSR2");
			}
			catch (error) {

				console.log(error);
			}
		});
	}

	async function stop() {

		// close listener
		await stopHttpServer(log, server, config.port);

		server.removeListener(
			"request",
			requestGatewayOnRequest
		);

		await publishRedis.quitAsync();

		if (createCacheRedis === true) {
			await cacheRedis.quitAsync();
		}
	}

	start().catch(error => {

		console.log(error);
	});
}

function hostService({
	name,
	configs,
	api,
	createService
}) {

	const env = process.env.FIYUU_ENV;
	if (env === undefined) {
		throw new Error("define FIYUU_ENV!");
	}

	const config = configs[env];
	if (config === undefined) {
		throw new Error("config not found.");
	}

	if (process.env.PORT) {
		config.port = process.env.PORT;
	}

	const simpleLogService = new SimpleLogService();

	const stdoutAppender = new StdoutAppender();
	simpleLogService.appenders.push(
		stdoutAppender
	);

	function createLog(category) {

		const log = new Log();
		log.service = simpleLogService;
		log.category = category;

		return log;
	}

	const log = createLog("host");

	log.trace(
		"create redis appender..."
	);

	const redisAppender = new RedisAppender();
	redisAppender.app = name;
	redisAppender.env = env;
	redisAppender.channel = "livelog";

	simpleLogService.appenders.push(
		redisAppender
	);

	log.trace(
		"require aws-sdk..."
	);

	const aws = require("aws-sdk");

	log.trace(
		"configure aws-sdk..."
	);

	aws.config.update({
		region: "eu-west-1"
	});

	const ddb = new aws.DynamoDB.DocumentClient(
		config.ddbOptions
	);

	const sqs = new aws.SQS(
		config.sqsOptions
	);

	const da = new DataAccess();
	da.log = createLog("da");
	da.tableNamePrefix = config.tableNamePrefix;
	da.ddb = ddb;

	const instances = createService({
		createLog,
		da,
		sqs
	});

	for (const instanceName in instances) {

		const instance = instances[instanceName];

		const instanceConfig = config[instanceName];
		if (instanceConfig === undefined) {
			continue;
		}

		for (const propertyName in instanceConfig) {

			const value = instanceConfig[
				propertyName
			];

			if (instance[propertyName] === undefined) {
				throw new Error();
			}

			instance[propertyName] = value;
		}
	}

	const httpapi = {

		GET: {

			"/": {
				handle(request, response) {
					response.statusCode = 200;
					response.end();
				}
			},

			"/health-check": {
				handle(request, response) {
					response.statusCode = 200;
					response.end();
				}
			}
		},
		POST: {
			...api.endpoints
		},
		OPTIONS: {
		}
	};

	const requestGateway = new RequestGateway();
	requestGateway.log = createLog("request-gateway");
	requestGateway.api = httpapi;
	requestGateway.authorizationService = {

		extract(request) {

			const principalId = request.headers["x-fiyuu-principal"];

			return {
				principalId
			};
		},

		authorize(request, authorizationInfo, serviceId, action, resource) {

			const {
				principalId
			} = authorizationInfo;

			return {
				principalId
			};
		}
	};

	requestGateway.instances = instances;

	const requestGatewayOnRequest = requestGateway.onRequest.bind(
		requestGateway
	);

	const server = http.createServer();

	let cacheRedis = null;
	let publishRedis = null;

	process.on("unhandledRejection", error => {
		log.error("unhandled rejection:", error);
	});

	let socketCount = 0;

	async function start() {

		cacheRedis = createRedisConnection(
			config.redisOptions,
			createLog("cache-redis")
		);

		publishRedis = createRedisConnection(
			config.redisOptions,
			createLog("publish-redis")
		);

		da.redis = cacheRedis;
		redisAppender.redis = publishRedis;

		server.on("connection", socket => {

			socketCount++;

			socket.once("close", hadError => {

				socketCount--;
			});
		});

		server.addListener(
			"request",
			requestGatewayOnRequest
		);

		await startHttpServer(log, server, config.port);


		process.once("SIGTERM", async () => {

			try {
				log.trace("stop...");
				await stop();

				log.trace("exit...");
				process.exit(0);
			}
			catch (error) {

				console.log(error);
			}
		});

		// nodemon restart handler
		process.once("SIGUSR2", async () => {

			try {
				log.trace("stop...");
				await stop();

				log.trace("kill...");
				process.kill(process.pid, "SIGUSR2");
			}
			catch (error) {

				console.log(error);
			}
		});
	}


	async function stop() {

		// close listener
		await stopHttpServer(log, server, config.port);

		server.removeListener(
			"request",
			requestGatewayOnRequest
		);

		await publishRedis.quitAsync();
		await cacheRedis.quitAsync();
	}

	start().catch(error => {

		console.log(error);
	});
}


class Worker {

	async run() {

		for (; ;) {

			let receiveMessageResponse;

			const receiveMessageRequest = this.sqs.receiveMessage({
				QueueUrl: this.queueUrl,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds: 10
			});

			this.receiveMessageRequest = receiveMessageRequest;

			try {

				try {
					receiveMessageResponse = await receiveMessageRequest.promise();
				}
				finally {
					this.receiveMessageRequest = null;
				}
			}
			catch (error) {

				this.log.warn("cannot receive message:", error);

				await tools.delay(30 * 1000);
				continue;
			}

			if (tools.isObject(receiveMessageResponse)) {
				// ok
			}
			else {

				this.log.warn(
					"receiveMessageResponse is not an object."
				);
				continue;
			}

			const messages = receiveMessageResponse.Messages;
			if (messages === undefined) {
				continue;
			}

			if (tools.isArray(messages)) {
				// ok
			}
			else {

				this.log.warn("messages is not an array.");
				continue;
			}

			if (0 < messages.length) {
				// ok
			}
			else {

				this.log.warn(
					"messages.length is not greater than 0."
				);

				continue;
			}

			this.log.trace(
				"received %d message(s).",
				messages.length
			);

			const message = messages[0];

			await this.processMessage(
				message
			);
		}
	}

	async processMessage(message) {

		try {

			if (tools.isObject(message)) {
				// ok
			}
			else {

				this.log.warn(
					"message is not an object."
				);

				throw new Error("invalid-message");
			}

			const {
				MessageId: messageId,
				Body: body
			} = message;

			if (tools.isString(body)) {
				// ok
			}
			else {

				this.log.warn(
					"body is not a string."
				);

				throw new Error("invalid-message");
			}

			let item;

			try {
				item = JSON.parse(
					body
				);
			}
			catch (error) {

				this.log.warn(
					"cannot parse body:",
					error
				);

				throw new Error("invalid-message");
			}

			if (tools.isObject(item)) {
				// ok
			}
			else {

				this.log.warn(
					"item is not an object."
				);

				throw new Error("invalid-message");
			}

			const {
				type,
				headers,
				content
			} = item;

			if (tools.isString(type)) {
				// ok
			}
			else {

				this.log.warn(
					"type is not a string."
				);

				throw new Error("invalid-message");
			}

			this.log.debug(
				"type is %s",
				type
			);

			const handler = this.api[type];

			if (handler === undefined) {

				this.log.warn(
					"handler is undefined."
				);

				throw new Error("invalid-message");
			}

			let principalId;

			if (headers === undefined) {
				// ok
			}
			else if (tools.isObject(headers)) {

				principalId = headers.principalId;
				if (principalId === undefined) {
					// ok
				}
				else if (tools.isCode(principalId)) {
					// ok
				}
				else {

					this.log.warn(
						"principalId is not a code."
					);

					throw new Error("invalid-message");
				}
			}
			else {

				this.log.warn(
					"headers is not an object."
				);

				throw new Error("invalid-message");
			}


			if (handler.request === undefined) {
				// ok
			}
			else {

				const errors = validate(
					handler.request,
					content,
					"content"
				);

				if (errors === undefined) {
					// ok
				}
				else {

					for (const error of errors) {
						this.log.warn(
							error
						);
					}

					throw new Error("invalid-message");
				}
			}

			const instanceName = handler.instance;
			const methodName = handler.method;

			const instance = this.instances[instanceName];

			this.log.trace(
				"process message %j...",
				messageId
			);

			try {

				await instance[methodName](
					{ principalId },
					content
				);
			}
			catch (error) {

				if (handler.faults && handler.faults[error.message] === null) {

					this.log.warn(
						"message is reported as invalid."
					);

					throw new Error("invalid-message");
				}
				else {

					this.log.warn(error);
				}
			}
		}
		catch (error) {

			switch (error.message) {

				case "invalid-message":
					break;

				case "retry":
					return;

				default:
					this.log.error(error);
					return;
			}
		}

		await this.deleteMessage(
			message
		);
	}

	async deleteMessage(message) {

		const {
			MessageId: messageId,
			ReceiptHandle: receiptHandle
		} = message;

		for (let i = 0; i < 10; i++) {

			this.log.trace(
				"delete message %j...",
				messageId
			);

			try {
				await this.sqs.deleteMessage({
					QueueUrl: this.queueUrl,
					ReceiptHandle: receiptHandle
				}).promise();
			}
			catch (error) {

				this.log.warn(
					"cannot delete message:",
					error
				);

				await tools.delay(1 * 1000);
				continue;
			}

			break;
		}
	}
}

Worker.prototype.log = null;
Worker.prototype.sqs = null;
Worker.prototype.sns = null;
Worker.prototype.queueUrl = null;
Worker.prototype.receiveMessageRequest = null;
Worker.prototype.api = null;
Worker.prototype.instances = null;


function hostWorker({
	name,
	configs,
	api,
	createService
}) {

	const env = process.env.FIYUU_ENV;
	if (env === undefined) {
		throw new Error("define FIYUU_ENV!");
	}

	const config = configs[env];
	if (config === undefined) {
		throw new Error("config not found.");
	}

	if (process.env.PORT) {
		config.port = process.env.PORT;
	}

	const simpleLogService = new SimpleLogService();
	const stdoutAppender = new StdoutAppender();

	simpleLogService.appenders.push(
		stdoutAppender
	);

	function createLog(category) {

		const log = new Log();
		log.service = simpleLogService;
		log.category = category;

		return log;
	}

	const log = createLog("host");

	log.trace(
		"require aws-sdk..."
	);

	const aws = require("aws-sdk");

	log.trace(
		"configure aws-sdk..."
	);

	aws.config.update({
		region: "eu-west-1"
	});

	const ddb = new aws.DynamoDB.DocumentClient(
		config.ddbOptions
	);

	const sqs = new aws.SQS(
		config.sqsOptions
	);

	const sns = new aws.SNS();
	const redisAppender = new RedisAppender();
	redisAppender.app = name;
	redisAppender.env = env;
	redisAppender.channel = "livelog";

	simpleLogService.appenders.push(
		redisAppender
	);

	const da = new DataAccess();
	da.log = createLog("da");
	da.tableNamePrefix = config.tableNamePrefix;
	da.ddb = ddb;


	const instances = createService({
		createLog,
		da,
		sqs
	});

	for (const instanceName in instances) {

		const instance = instances[instanceName];

		const instanceConfig = config[instanceName];
		if (instanceConfig === undefined) {
			continue;
		}

		for (const propertyName in instanceConfig) {

			const value = instanceConfig[
				propertyName
			];

			if (instance[propertyName] === undefined) {
				throw new Error();
			}

			instance[propertyName] = value;
		}
	}

	let cacheRedis = null;
	let publishRedis = null;

	process.on("unhandledRejection", error => {
		log.error("unhandled rejection:", error);
	});

	const workerapi = {
		...api.endpoints
	};

	const worker = new Worker();
	worker.log = createLog("worker");
	worker.sqs = sqs;
	worker.sns = sns;
	worker.queueUrl = config.queue;
	worker.api = workerapi;
	worker.instances = instances;

	async function start() {

		cacheRedis = createRedisConnection(
			config.redisOptions,
			createLog("cache-redis")
		);

		publishRedis = createRedisConnection(
			config.redisOptions,
			createLog("publish-redis")
		);

		da.redis = cacheRedis;
		redisAppender.redis = publishRedis;

		process.once("SIGTERM", async () => {

			try {
				log.trace("stop...");
				await stop();

				log.trace("exit...");
				process.exit(0);
			}
			catch (error) {

				console.log(error);
			}
		});

		// nodemon restart handler
		process.once("SIGUSR2", async () => {

			try {
				log.trace("stop...");
				await stop();

				log.trace("kill...");
				process.kill(process.pid, "SIGUSR2");
			}
			catch (error) {

				console.log(error);
			}
		});

		worker.run();
	}

	async function stop() {

		// stop worker

		await publishRedis.quitAsync();
		await cacheRedis.quitAsync();
	}

	start().catch(error => {

		console.log(error);
	});
}

module.exports = {
	RequestServiceClient,
	AccessServiceClient,
	ProductServiceClient,
	AuthorizationService,
	startHttpServer,
	stopHttpServer,
	hostAPI,
	hostService,
	hostWorker,
	Worker
};
