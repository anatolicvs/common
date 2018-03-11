"use strict";
const tools = require("./tools.js");

class SimpleTrackService {

	constructor() {
		this.appenders = [];
	}

	track(source, event, content) {

		const ts = tools.ts();

		for (const appender of this.appenders) {

			appender.append(
				ts,
				source,
				event,
				content
			);
		}
	}
}

SimpleTrackService.prototype.appenders = null;

class Track {

	track(event, content) {

		this.service.track(
			this.source,
			event,
			content
		);
	}
}

Track.prototype.service = null;
Track.prototype.source = null;

class NoTrack {

	track() {
	}
}

NoTrack.instance = new NoTrack();

class ConsoleTrack {

	track(event, content) {

		console.log("\x1b[38;5;240mTRACK\x1b[0m", event);
	}
}

ConsoleTrack.instance = new ConsoleTrack();

class StdoutTrackAppender {

	append(ts, source, event, content) {

		const {
			stdout
		} = process;

		if (stdout.isTTY) {
			stdout.write(`${source} ${event}`);
		}
		else {
			stdout.write(`${source} ${event}`);
		}
	}
}

class RedisTrackAppender {

	append(ts, source, event, content) {

		const redis = this.redis;

		if (redis.connected) {

			const {
				app,
				env,
				channel
			} = this;

			const json = JSON.stringify({
				ts,
				app,
				env,
				source,
				event,
				content
			});

			redis.publish(channel, json, (error, reply) => {

			});
		}
	}
}

RedisTrackAppender.prototype.app = null;
RedisTrackAppender.prototype.env = null;
RedisTrackAppender.prototype.channel = null;
RedisTrackAppender.prototype.redis = null;

module.exports = {
	SimpleTrackService,
	StdoutTrackAppender,
	RedisTrackAppender,
	Track,
	NoTrack,
	ConsoleTrack
};
