"use strict";
const redis = require("redis");
const bluebird = require("bluebird");
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

function createRedisConnection(options, log) {

	const client = redis.createClient(
		options
	);

	client.on("ready", () => {
		log.info("connected to redis.");
	});

	client.on("reconnecting", data => {
		log.info("trying to reconnect to redis %d. time after %dms...", data.attempt, data.delay);
	});

	client.on("warning", e => {
		log.error("redis.warning: ", e);
	});

	client.on("error", err => {
		log.warn(err.message);
	});

	return client;
}

module.exports = {
	createRedisConnection
};