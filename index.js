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

const {
	RepositoryGenerator
} = require("./src/RepositoryGenerator");

module.exports = {
	tools,

	SimpleLogService,
	StdoutAppender,
	RedisAppender,
	DynamoDbAppender,
	Log,
	NoLog,
	ConsoleLog,

	RepositoryGenerator
};
