"use strict";
const tools = require("./src/tools.js");

const {
	SimpleLogService,
	StdoutAppender,
	RedisAppender,
	DynamoDbAppender,
	Log,
	NoLog,
	ConsoleLog
} = require("./src/logging.js");

module.exports = {
	tools,

	...require("./src/logging.js"),
	...require("./src/RepositoryGenerator"),

	...require("./src/DiscoveryService"),
	...require("./src/createRedisConnection"),

	...require("./src/createHttpServer"),
	...require("./src/ServiceClientBase"),
	...require("./src/RepositoryGenerator2"),
	...require("./src/validate")
};
