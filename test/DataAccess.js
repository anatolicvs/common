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
	},

	{
		TableName: "prefix.user-logins",
		KeySchema: [
			{ AttributeName: "id", KeyType: "HASH" },
			{ AttributeName: "ts", KeyType: "RANGE" }
		],
		AttributeDefinitions: [
			{ AttributeName: "id", AttributeType: "S" },
			{ AttributeName: "ts", AttributeType: "N" }
		],
		ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
	}
];

mockDynamoDB.items = {
	"prefix.users": [
		{ id: "user-1", iv: 0 },
		{ id: "user-2", iv: 30 },
		{ id: "user-3", iv: 1 }
	],
	"prefix.groups": [
		{ id: "group-1", iv: 0 },
		{ id: "group-2", iv: 0 },
		{ id: "group-3", iv: 0 }
	],
	"prefix.group-user-pairs": [
		{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 },
		{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", iv: 0 }
	],
	"prefix.user-logins": [
		{ id: "user-1", ts: 0 },
		{ id: "user-1", ts: 1 },
		{ id: "user-1", ts: 2 },
		{ id: "user-2", ts: 0 },
		{ id: "user-2", ts: 1 }
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

		await redisClient.quitAsync();
		await redisServer.close();
		await mockDynamoDB.stop();
	});


	beforeEach(async () => {
		await redisClient.flushallAsync();
	});

	async function assertRedisEmpty() {

		assert.deepStrictEqual(
			await redisClient.keysAsync("*"),
			[]
		);
	}

	it("get", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await dba.get("users", "id", "user-1"),
			{
				id: "user-1",
				iv: 0
			}
		);

		await assertRedisEmpty();
	});

	it("get null item", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await dba.get("users", "id", "user-0"),
			undefined
		);

		await assertRedisEmpty();
	});

	it("get null table", async () => {

		await assertRedisEmpty();

		try {
			await dba.get("no-users", "id", "user-1");
		}
		catch (error) {

			assert(error.code === "ResourceNotFoundException");
			await assertRedisEmpty();
			return;
		}

		throw new Error();
	});

	it("get-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.getCached(ttl, "users", "id", "user-1"),
			{
				id: "user-1",
				iv: 0
			}
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.users!user-1"),
			ttl
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.users!user-1")),
			{ id: "user-1", iv: 0 }
		);
	});

	it("get-cached null", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await dba.getCached(600, "users", "id", "user-0"),
			undefined
		);

		await assertRedisEmpty();
	});

	it("get-cached-versioned", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.getCachedVersioned(ttl, "users", "id", "user-1", "iv"),
			{ id: "user-1", iv: 0 }
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.users!user-1"),
			ttl
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.users!user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "user-1", iv: 0 },
				0
			]
		);
	});

	it("get-cached-versioned null", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await dba.getCachedVersioned(10, "users", "id", "user-0", "iv"),
			undefined
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.users!user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[]
		);
	});

	it("query-table-ranged-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.queryTableRangedCached(ttl, "user-logins", "id", "ts", "user-1"),
			[
				{ id: 'user-1', ts: 0 },
				{ id: 'user-1', ts: 1 },
				{ id: 'user-1', ts: 2 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1!0"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1!1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1!2"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.zrangeAsync("prefix.user-logins!user-1", 0, -1, "WITHSCORES"),
			[
				"user-1!0",
				"0",
				"user-1!1",
				"1",
				"user-1!2",
				"2"
			]
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-1!0")),
			{ id: 'user-1', ts: 0 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-1!1")),
			{ id: 'user-1', ts: 1 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-1!2")),
			{ id: 'user-1', ts: 2 }
		);
	});

	it("scan-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.scanCached(ttl, "group-user-pairs", "id"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-2|user-1"),
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
			JSON.parse(await redisClient.getAsync("prefix.group-user-pairs!group-1|user-1")),
			{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.group-user-pairs!group-2|user-1")),
			{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", iv: 0 }
		);

		assert.deepStrictEqual(
			await dba.scanCached(ttl, "group-user-pairs", "id"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);
	});

	it("scan-ranged-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.scanRangedCached(ttl, "user-logins", "id", "ts"),
			[
				{ id: "user-2", ts: 0 },
				{ id: "user-2", ts: 1 },
				{ id: "user-1", ts: 0 },
				{ id: "user-1", ts: 1 },
				{ id: "user-1", ts: 2 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1!0"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1!1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-1!2"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-2!0"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.user-logins!user-2!1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.zrangeAsync("prefix.user-logins", 0, -1, "WITHSCORES"),
			[
				"user-2!0",
				"0",
				"user-2!1",
				"1",
				"user-1!0",
				"2",
				"user-1!1",
				"3",
				"user-1!2",
				"4"
			]
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-1!0")),
			{ id: 'user-1', ts: 0 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-1!1")),
			{ id: 'user-1', ts: 1 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-1!2")),
			{ id: 'user-1', ts: 2 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-2!0")),
			{ id: 'user-2', ts: 0 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.user-logins!user-2!1")),
			{ id: 'user-2', ts: 1 }
		);
	});

	it("scan-cached-versioned", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.scanCachedVersioned(ttl, "group-user-pairs", "id", undefined, "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-2|user-1"),
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
				{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-2|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", iv: 0 },
				0
			]
		);
	});

	it("query-index-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!userId-createdAt-index!user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-2|user-1"),
			ttl
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
			JSON.parse(await redisClient.getAsync("prefix.group-user-pairs!group-1|user-1")),
			{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 }
		);

		assert.deepStrictEqual(
			JSON.parse(await redisClient.getAsync("prefix.group-user-pairs!group-2|user-1")),
			{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", iv: 0 }
		);

		assert.deepStrictEqual(
			await dba.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);
	});

	it("query-index-cached-versioned", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await dba.queryIndexCachedVersioned(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!userId-createdAt-index!user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-2|user-1"),
			ttl
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
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-1|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 },
				0
			]
		);

		assert.deepStrictEqual(
			(await redisClient.zrangeAsync("prefix.group-user-pairs!group-2|user-1", 0, -1, "WITHSCORES")).map(JSON.parse),
			[
				{ id: "group-2|user-1", createdAt: 0, groupId: "group-2", userId: "user-1", iv: 0 },
				0
			]
		);
	});
});
