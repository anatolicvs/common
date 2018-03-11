"use strict";
const tools = require("./src/tools.js");

module.exports = {
	tools,

	...require("./src/logging.js"),
	tracking: require("./src/tracking.js"),
	...require("./src/RepositoryGenerator"),

	...require("./src/DiscoveryService"),
	...require("./src/createRedisConnection"),

	...require("./src/createHttpServer"),
	...require("./src/ServiceClientBase"),
	...require("./src/RepositoryGenerator2"),
	...require("./src/DataAccess"),
	...require("./src/validate"),
	...require("./src/JWTService")
};
