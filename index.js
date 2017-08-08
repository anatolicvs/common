"use strict";
const tools = require("./tools.js");

const {
	SimpleLogService,
	StdoutAppender,
	RedisAppender,
	DynamoDbAppender,
	Log,
	NoLog,
	ConsoleLog
} = require("./Log.js");

module.exports = {
	tools,

	SimpleLogService,
	StdoutAppender,
	RedisAppender,
	DynamoDbAppender,
	Log,
	NoLog,
	ConsoleLog
};
