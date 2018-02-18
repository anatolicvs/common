"use strict";
const assert = require('assert');
const redis = require("redis");
const RedisServer = require('redis-server');
const bluebird = require("bluebird");

const { NoLog, ConsoleLog, tools } = require("..");
const { DataAccess } = require("../src/DataAccess");
const { MockDynamoDB } = require("./MockDynamoDB.js");

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const mockDynamoDB = new MockDynamoDB();
mockDynamoDB.port = 15554;
mockDynamoDB.createTableRequests = [
	{
		TableName: "prefix.users",
		KeySchema: [
			{ AttributeName: "id", KeyType: "HASH" }
		],
		AttributeDefinitions: [
			{ AttributeName: "id", AttributeType: "S" }
		],
		ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
	},

	{
		TableName: "prefix.groups",
		KeySchema: [
			{ AttributeName: "id", KeyType: "HASH" }
		],
		AttributeDefinitions: [
			{ AttributeName: "id", AttributeType: "S" }
		],
		ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
	},

	{
		TableName: "prefix.group-user-pairs",
		KeySchema: [
			{ AttributeName: "id", KeyType: "HASH" }
		],
		GlobalSecondaryIndexes: [
			{
				IndexName: "groupId-createdAt-index",
				KeySchema: [
					{ AttributeName: "groupId", KeyType: "HASH" },
					{ AttributeName: "createdAt", KeyType: "RANGE" }
				],
				Projection: { ProjectionType: "ALL" },
				ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
			},
			{
				IndexName: "userId-createdAt-index",
				KeySchema: [
					{ AttributeName: "userId", KeyType: "HASH" },
					{ AttributeName: "createdAt", KeyType: "RANGE" }
				],
				Projection: { ProjectionType: "ALL" },
				ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
			}
		],
		AttributeDefinitions: [
			{ AttributeName: "id", AttributeType: "S" },
			{ AttributeName: "groupId", AttributeType: "S" },
			{ AttributeName: "userId", AttributeType: "S" },
			{ AttributeName: "createdAt", AttributeType: "N" }
		],
		ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
	}
];

mockDynamoDB.items = {
	"prefix.users": [
		{ id: "user-1", __iv: 0 },
		{ id: "user-2", __iv: 30 },
		{ id: "user-3", __iv: 1 }
	],
	"prefix.groups": [
		{ id: "group-1", __iv: 0 },
		{ id: "group-2", __iv: 0 },
		{ id: "group-3", __iv: 0 }
	],
	"prefix.group-user-pairs": [
		{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", __iv: 0 },
		{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", __iv: 0 }
	]
};

let redisServer;
let redisClient;

describe("RepositoryGenerator", () => {

	const dba = new DataAccess();
	dba.log = NoLog.instance;
	dba.tableNamePrefix = "prefix.";

	before(async () => {

		await mockDynamoDB.start();

		redisServer = new RedisServer({
			port: 15555
		});

		await redisServer.open();

		redisClient = redis.createClient({
			port: 15555
		});

		await new Promise((resolve, reject) => {
			redisClient.once("connect", resolve);
		});

		dba.ddb = mockDynamoDB.ddb;
		dba.redis = redisClient;
	});

	after(async () => {

		dba.ddb = null;
		dba.redis = null;

		await new Promise((resolve, reject) => {
			redisClient.once("end", resolve);
			redisClient.quit();
		});

		await redisServer.close();
		await mockDynamoDB.stop();
	});


	beforeEach(async () => {
		await redisClient.flushallAsync();
	});

	it("get", async () => {

		assert.deepStrictEqual(
			await dba.get("users", "id", "user-1"),
			{
				id: "user-1",
				__iv: 0
			}
		);
	});

	it("get null item", async () => {

		assert.deepStrictEqual(
			await dba.get("users", "id", "user-0"),
			undefined
		);
	});

	it("get null table", async () => {

		try {
			await dba.get("no-users", "id", "user-1");
		}
		catch (error) {

			assert(error.code === "ResourceNotFoundException");
			return;
		}

		throw new Error();
	});

	it("get-cached", async () => {

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.getCached(ttl, "users", "id", "user-1"),
			{
				id: "user-1",
				__iv: 0
			}
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.users!user-1")),
			{ id: "user-1", __iv: 0 }
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.users!user-1"),
			ttl
		);
	});

	it("get-cached null", async () => {

		assert.deepStrictEqual(
			await dba.getCached(600, "users", "id", "user-0"),
			undefined
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.users!user-1")),
			null
		);
	});

	it("get-cached-versioned", async () => {

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.getCachedVersioned(ttl, "users", "id", "user-1", "__iv"),
			{
				id: "user-1",
				__iv: 0
			}
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.users!user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "user-1", __iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.users!user-1"),
			ttl
		);
	});

	it("get-cached-versioned null", async () => {

		assert.deepStrictEqual(
			await dba.getCachedVersioned(10, "users", "id", "user-0", "__iv"),
			undefined
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.users!user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[]
		);
	});

	it("scan-cached-versioned", async () => {

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.scanCachedVersioned(ttl, "group-user-pairs", "id", undefined, "__iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", __iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", __iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.zrangeAsync("prefix.group-user-pairs", 0, -1, "WITHSCORES"),
			[
				"group-1|user-1",
				"0",
				"group-2|user-1",
				"1"
			]
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-1|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", __iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-2|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", __iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-2|user-1"),
			ttl
		);
	});

	it("query-index-cached-versioned", async () => {

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.queryIndexCachedVersioned(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "__iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", __iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", __iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.zrangeAsync("prefix.group-user-pairs!userId-createdAt-index!user-1", 0, -1, "WITHSCORES"),
			[
				"group-1|user-1",
				"0",
				"group-2|user-1",
				"1"
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!userId-createdAt-index!user-1"),
			ttl
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-1|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", __iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-2|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", __iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-2|user-1"),
			ttl
		);
	});
});
