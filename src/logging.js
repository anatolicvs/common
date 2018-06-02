"use strict";
const os = require("os");
const tools = require("./tools.js");

class SimpleLogService {

	constructor() {
		this.appenders = [];
	}

	log(category, level, args) {

		const ts = tools.ts();
		const message = tools.format(...args);

		const {
			app,
			env,
			appenders
		} = this;

		for (const appender of appenders) {

			appenders[i].append(
				app,
				env,
				category,
				ts,
				level,
				message,
				args
			);
		}
	}
}

SimpleLogService.prototype.app = null;
SimpleLogService.prototype.env = null;
SimpleLogService.prototype.appenders = null;

class Log {

	log(level, args) {

		const enabled = level <= this.level;

		if (args.length === 0) {
			// ok
		}
		else if (enabled) {
			this.service.log(
				this.category,
				level,
				args
			);
		}

		return enabled;
	}

	trace(...args) {
		return this.log(6, args);
	}

	debug(...args) {
		return this.log(5, args);
	}

	info(...args) {
		return this.log(4, args);
	}

	warn(...args) {
		return this.log(3, args);
	}

	error(...args) {
		return this.log(2, args);
	}

	fatal(...args) {
		return this.log(1, args);
	}
}

Log.prototype.service = null;
Log.prototype.category = "general";
Log.prototype.level = 0x7FFFFFFF;

class NoLog {

	trace() {
		return false;
	}

	debug() {
		return false;
	}

	info() {
		return false;
	}

	warn() {
		return false;
	}

	error() {
		return false;
	}

	fatal() {
		return false;
	}
}

NoLog.instance = new NoLog();

class ConsoleLog {

	trace(...args) {
		console.log("\x1b[38;5;240mTRACE\x1b[0m", tools.format(...args));
		return true;
	}

	debug(...args) {
		console.log("DEBUG", tools.format(...args));
		return true;
	}

	info(...args) {
		console.log("INFO", tools.format(...args));
		return true;
	}

	warn(...args) {
		console.log("WARN", tools.format(...args));
		return true;
	}

	error(...args) {
		console.log("ERROR", tools.format(...args));
		return true;
	}

	fatal(...args) {
		console.log("FATAL", tools.format(...args));
		return true;
	}
}

ConsoleLog.instance = new ConsoleLog();

class StdoutAppender {

	getTitleEscape(level) {
		switch (level) {
			case 1:
				return "\x1b[35m";

			case 2:
				return "\x1b[31m";

			case 3:
				return "\x1b[33m";

			case 4:
				return "\x1b[38;5;28m";

			case 5:
				return "\x1b[38;5;25m";

			case 6:
				return "\x1b[38;5;240m";

			default:
				return "\x1b[1;30m";
		}
	}

	getBodyEscape(level) {
		switch (level) {
			case 1:
				return "\x1b[1;35m";

			case 2:
				return "\x1b[1;31m";

			case 3:
				return "\x1b[1;33m";

			case 4:
				return "\x1b[38;5;78m";

			case 5:
				return "\x1b[38;5;75m";

			case 6:
				return "\x1b[38;5;250m";

			default:
				return "\x1b[1;30m";
		}
	}

	getName(level) {
		switch (level) {
			case 1:
				return "FATAL";

			case 2:
				return "ERROR";

			case 3:
				return "WARN ";

			case 4:
				return "INFO ";

			case 5:
				return "DEBUG";

			case 6:
				return "TRACE";

			default:
				return "\x1b[30;1m";
		}
	}

	append(app, env, category, ts, level, message, args) {

		if (level < this.max) {

			const stdout = process.stdout;

			if (stdout.isTTY) {
				stdout.write(`${this.getTitleEscape(level)}${app} ${category}\x1b[0m ${this.getBodyEscape(level)}${message}\x1b[0m${os.EOL}`);
			}
			else {
				stdout.write(`${new Date(ts).toISOString()} ${this.getName(level)} ${category} ${message}${os.EOL}`);
			}
		}
	}
}

StdoutAppender.prototype.max = Infinity;

class RedisAppender {

	append(app, env, category, ts, level, message, args) {

		const redis = this.redis;

		if (redis.connected) {

			const {
				channel
			} = this;

			const json = JSON.stringify({
				app,
				env,
				category,
				ts,
				level,
				message
			});

			redis.publish(channel, json, (error, reply) => {
				// ok
			});
		}
	}
}

RedisAppender.prototype.channel = null;
RedisAppender.prototype.redis = null;

class DynamoDbAppender {

	append(app, env, category, ts, level, message, args) {

		if (this.level < level) {
		}
		else {
			this.ddb.put({
				TableName: this.tableName,
				Item: {
					app,
					env,
					category,
					ts,
					level,
					message
				}
			}).promise().catch(error => {
				console.log(error);
			});
		}
	}
}

DynamoDbAppender.prototype.ddb = null;
DynamoDbAppender.prototype.tableName = null;
DynamoDbAppender.prototype.level = null;

module.exports = {
	SimpleLogService,
	StdoutAppender,
	RedisAppender,
	DynamoDbAppender,
	Log,
	NoLog,
	ConsoleLog
};