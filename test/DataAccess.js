"use strict";
const assert = require('assert');
const redis = require("redis");
const RedisServer = require('redis-server');
const bluebird = require("bluebird");

const { NoLog } = require("..");
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
	},

	{
		TableName: "prefix.user-items",
		KeySchema: [
			{ AttributeName: "id", KeyType: "HASH" }
		],
		AttributeDefinitions: [
			{ AttributeName: "id", AttributeType: "S" },
			{ AttributeName: "userId", AttributeType: "S" },
			{ AttributeName: "createdAt", AttributeType: "N" }
		],
		GlobalSecondaryIndexes: [
			{
				IndexName: "userId-createdAt-index",
				KeySchema: [
					{ AttributeName: "userId", KeyType: "HASH" },
					{ AttributeName: "createdAt", KeyType: "RANGE" }
				],
				Projection: { ProjectionType: "ALL" },
				ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
			},
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
	],
	"prefix.user-items": [
		{ id: "item-0", createdAt: 0, userId: "user-1" },
		{ id: "item-1", createdAt: 1, userId: "user-1" },
		{ id: "item-2", createdAt: 2, userId: "user-1" },
		{ id: "item-3", createdAt: 3, userId: "user-1" },
		{ id: "item-4", createdAt: 4, userId: "user-1" },
		{ id: "item-5", createdAt: 5, userId: "user-1" },
		{ id: "item-6", createdAt: 6, userId: "user-1" },
		{ id: "item-7", createdAt: 7, userId: "user-1" },
		{ id: "item-8", createdAt: 8, userId: "user-1" },
		{ id: "item-9", createdAt: 9, userId: "user-1" },
		{ id: "item-10", createdAt: 10, userId: "user-2" },
		{ id: "item-11", createdAt: 11, userId: "user-2" },
		{ id: "item-12", createdAt: 12, userId: "user-2" },
		{ id: "item-13", createdAt: 13, userId: "user-2" },
		{ id: "item-14", createdAt: 14, userId: "user-2" },
		{ id: "item-15", createdAt: 15, userId: "user-2" },
		{ id: "item-16", createdAt: 16, userId: "user-2" },
		{ id: "item-17", createdAt: 17, userId: "user-2" },
		{ id: "item-18", createdAt: 18, userId: "user-2" },
		{ id: "item-19", createdAt: 19, userId: "user-2" },
	]
};

let redisServer;
let redisClient;

describe("DataAccess", () => {

	const da = new DataAccess();
	da.log = NoLog.instance;
	da.tableNamePrefix = "prefix.";

	before(async () => {

	});

	after(async () => {

	});


	beforeEach(async () => {

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

		da.ddb = mockDynamoDB.ddb;
		da.redis = redisClient;

		await redisClient.flushallAsync();
	});

	afterEach(async () => {

		da.ddb = null;
		da.redis = null;

		await redisClient.quitAsync();
		await redisServer.close();
		await mockDynamoDB.stop();
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
			await da.get("users", "id", "user-1"),
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
			await da.get("users", "id", "user-0"),
			undefined
		);

		await assertRedisEmpty();
	});

	it("get null table", async () => {

		await assertRedisEmpty();

		try {
			await da.get("no-users", "id", "user-1");
		}
		catch (error) {

			assert(error.code === "ResourceNotFoundException");
			await assertRedisEmpty();
			return;
		}

		throw new Error();
	});

	it("batch-get", async () => {

		await assertRedisEmpty();

		await da.batchGet("users", "id", ["user-1", "user-2"]);
	});

	it("get-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await da.getCached(ttl, "users", "id", "user-1"),
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
			await da.getCached(600, "users", "id", "user-0"),
			undefined
		);

		await assertRedisEmpty();
	});

	it("get-cached-versioned", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await da.getCachedVersioned(ttl, "users", "id", "user-1", "iv"),
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
			await da.getCachedVersioned(10, "users", "id", "user-0", "iv"),
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
			await da.queryTableRangedCached(ttl, "user-logins", "id", "ts", "user-1"),
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

	it("scan", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.scan("group-user-pairs"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		await assertRedisEmpty();
	});

	it("enumerate-table", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.enumerateTable("group-user-pairs", 1),
			{
				items: [
					{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: 'user-1', iv: 0 }
				],
				lastEvaluatedKey: { id: "group-1|user-1" }
			}
		);

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.enumerateTable("group-user-pairs", 1, { id: "group-1|user-1" }),
			{
				items: [
					{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
				],
				lastEvaluatedKey: { id: "group-2|user-1" }
			}
		);

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.enumerateTable("group-user-pairs", 1, { id: "group-2|user-1" }),
			{
				items: []
			}
		);

		await assertRedisEmpty();
	});

	describe("enumerate-index-ranged", () => {

		it("limit<n,asc", async () => {

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 2, undefined, false),
				{
					items: [
						{ id: 'item-0', createdAt: 0, userId: 'user-1' },
						{ id: 'item-1', createdAt: 1, userId: 'user-1' },
					],
					lastEvaluatedKey: { id: "item-1", createdAt: 1, userId: "user-1" }
				}
			);

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 2, { id: "item-1", createdAt: 1, userId: "user-1" }, false),
				{
					items: [
						{ id: 'item-2', createdAt: 2, userId: 'user-1' },
						{ id: 'item-3', createdAt: 3, userId: 'user-1' },
					],
					lastEvaluatedKey: { id: "item-3", createdAt: 3, userId: "user-1" }
				}
			);

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 2, { id: "item-3", createdAt: 3, userId: "user-1" }, false),
				{
					items: []
				}
			);
		});

		it("limit<n,desc", async () => {

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 2, undefined, true),
				{
					items: [
						{ id: 'item-3', createdAt: 3, userId: 'user-1' },
						{ id: 'item-2', createdAt: 2, userId: 'user-1' },
					],
					lastEvaluatedKey: { id: "item-2", createdAt: 2, userId: "user-1" }
				}
			);

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 2, { id: "item-2", createdAt: 2, userId: "user-1" }, true),
				{
					items: [
						{ id: 'item-1', createdAt: 1, userId: 'user-1' },
						{ id: 'item-0', createdAt: 0, userId: 'user-1' },
					],
					lastEvaluatedKey: { id: "item-0", createdAt: 0, userId: "user-1" }
				}
			);

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 2, { id: "item-0", createdAt: 0, userId: "user-1" }, true),
				{
					items: []
				}
			);
		});

		it("limit=n,asc", async () => {

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 4, undefined, false),
				{
					items: [
						{ id: 'item-0', createdAt: 0, userId: 'user-1' },
						{ id: 'item-1', createdAt: 1, userId: 'user-1' },
						{ id: 'item-2', createdAt: 2, userId: 'user-1' },
						{ id: 'item-3', createdAt: 3, userId: 'user-1' }
					],
					lastEvaluatedKey: { id: "item-3", createdAt: 3, userId: "user-1" }
				}
			);

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 4, { id: "item-3", createdAt: 3, userId: "user-1" }, false),
				{
					items: []
				}
			);
		});

		it("limit=n,desc", async () => {

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 4, undefined, true),
				{
					items: [
						{ id: 'item-3', createdAt: 3, userId: 'user-1' },
						{ id: 'item-2', createdAt: 2, userId: 'user-1' },
						{ id: 'item-1', createdAt: 1, userId: 'user-1' },
						{ id: 'item-0', createdAt: 0, userId: 'user-1' },
					],
					lastEvaluatedKey: { id: "item-0", createdAt: 0, userId: "user-1" }
				}
			);

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 4, { id: "item-0", createdAt: 0, userId: "user-1" }, true),
				{
					items: []
				}
			);
		});

		it("limit>n,asc", async () => {

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 5, undefined, false),
				{
					items: [
						{ id: 'item-0', createdAt: 0, userId: 'user-1' },
						{ id: 'item-1', createdAt: 1, userId: 'user-1' },
						{ id: 'item-2', createdAt: 2, userId: 'user-1' },
						{ id: 'item-3', createdAt: 3, userId: 'user-1' }
					]
				}
			);
		});

		it("limit>n,desc", async () => {

			assert.deepStrictEqual(
				await da.enumerateIndexRanged("user-items", "userId-createdAt-index", "userId", "user-1", "createdAt", 0, 3, 5, undefined, true),
				{
					items: [
						{ id: 'item-3', createdAt: 3, userId: 'user-1' },
						{ id: 'item-2', createdAt: 2, userId: 'user-1' },
						{ id: 'item-1', createdAt: 1, userId: 'user-1' },
						{ id: 'item-0', createdAt: 0, userId: 'user-1' }
					]
				}
			);
		});
	});

	it("scan-cached", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await da.scanCached(ttl, "group-user-pairs", "id"),
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
			await da.scanCached(ttl, "group-user-pairs", "id"),
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
			await da.scanRangedCached(ttl, "user-logins", "id", "ts"),
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
			await da.scanCachedVersioned(ttl, "group-user-pairs", "id", undefined, "iv"),
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
			await da.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
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
			await da.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);
	});

	it("query-index-cached should check item[indexHashName]", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await da.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
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
			await da.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await da.getCached(ttl, "group-user-pairs", "id", "group-1|user-1"),
			{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 },
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		await da.updateCached(ttl, "group-user-pairs", "id", undefined, {
			id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-2", iv: 0
		});

		assert.deepStrictEqual(
			await da.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-2"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-2", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await da.queryIndexCached(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1"),
			[
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);


	});

	it("query-index-cached-versioned", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
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

	it("query-index-cached-versioned should check item[indexHashName]", async () => {

		await assertRedisEmpty();

		const ttl = 10 + Math.floor(Math.random() * 10);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
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

		assert.deepStrictEqual(
			await da.getCachedVersioned(ttl, "group-user-pairs", "id", "group-1|user-1", "iv"),
			{ id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-1", iv: 0 },
		);

		assert.deepStrictEqual(
			await redisClient.ttlAsync("prefix.group-user-pairs!group-1|user-1"),
			ttl
		);

		await da.updateCachedVersioned(ttl, "group-user-pairs", "id", undefined, "iv", {
			id: "group-1|user-1", createdAt: 0, groupId: "group-1", userId: "user-2", iv: 0
		});

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-2", "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-2", iv: 1 }
			]
		);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(ttl, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
			[
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);
	});

	it("query-index-first", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.queryIndexFirst("group-user-pairs", "userId-createdAt-index", "userId", "user-1", false),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 }
			]
		);

		await assertRedisEmpty();
	});

	it("query-index-first desc", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.queryIndexFirst("group-user-pairs", "userId-createdAt-index", "userId", "user-1", true),
			[
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		await assertRedisEmpty();
	});

	it("create-cached-versioned", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.scanCachedVersioned(10, "group-user-pairs", "id", undefined, "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(10, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		await da.createCachedVersioned(10, "group-user-pairs", "id", undefined, "iv", {
			id: "group-10|user-1",
			createdAt: 0,
			groupId: "group-10",
			userId: "user-1"
		}, {
				"userId-createdAt-index": { hash: "userId", range: "createdAt" }
			});

		assert.deepStrictEqual(
			await da.scanCachedVersioned(10, "group-user-pairs", "id", undefined, "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-10|user-1', createdAt: 0, groupId: 'group-10', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 },
			]
		);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(10, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-10|user-1', createdAt: 0, groupId: 'group-10', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);
	});

	it("remove-cached-versioned", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.scanCachedVersioned(10, "group-user-pairs", "id", undefined, "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(10, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 },
				{ id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0 }
			]
		);

		await da.removeCachedVersioned("group-user-pairs", "id", undefined, "iv", {
			id: 'group-2|user-1', createdAt: 0, groupId: 'group-2', userId: "user-1", iv: 0
		});

		// {
		// 	"userId-createdAt-index": { hash: "userId", range: "createdAt" }
		// }

		assert.deepStrictEqual(
			await da.scanCachedVersioned(10, "group-user-pairs", "id", undefined, "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 }
			]
		);

		assert.deepStrictEqual(
			await da.queryIndexCachedVersioned(10, "group-user-pairs", "id", undefined, "userId-createdAt-index", "userId", "user-1", "iv"),
			[
				{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: "user-1", iv: 0 }
			]
		);
	});

	it("create-or-get create", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.createOrGet("group-user-pairs", "id", { id: "group-3|user-1" }),
			undefined
		);

		await assertRedisEmpty();
	});

	it("create-or-get get", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.createOrGet("group-user-pairs", "id", { id: "group-1|user-1" }),
			{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: 'user-1', iv: 0 }
		);

		await assertRedisEmpty();
	});

	it("get-or-create get", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.getOrCreate("group-user-pairs", "id", { id: "group-1|user-1" }),
			{ id: 'group-1|user-1', createdAt: 0, groupId: 'group-1', userId: 'user-1', iv: 0 }
		);

		await assertRedisEmpty();
	});

	it("get-or-create create", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.getOrCreate("group-user-pairs", "id", { id: "group-4|user-1" }),
			undefined
		);

		await assertRedisEmpty();
	});

	it("create", async () => {

		await assertRedisEmpty();

		assert.deepStrictEqual(
			await da.create("users", "id", { id: "user-30" }),
			undefined
		);

		await assertRedisEmpty();
	});
});
