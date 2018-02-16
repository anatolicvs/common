"use strict";
const assert = require('assert');
const redis = require("redis");
const RedisServer = require('redis-server');
const bluebird = require("bluebird");

const { NoLog, ConsoleLog, tools } = require("..");
const { DynamoRedisAccess } = require("../src/DynamoRedisAccess");
const { MockDynamoDB } = require("./MockDynamoDB.js");

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const mockDynamoDB = new MockDynamoDB();
mockDynamoDB.port = 15554;
mockDynamoDB.createTableRequests = [
	{
		TableName: "prefix.users",
		KeySchema: [{
			AttributeName: "id",
			KeyType: "HASH",
		}],
		AttributeDefinitions: [
			{
				AttributeName: "id",
				AttributeType: "S"
			},
			{
				AttributeName: "groupId",
				AttributeType: "S"
			}
		],
		ProvisionedThroughput: {
			ReadCapacityUnits: 1,
			WriteCapacityUnits: 1
		},
		GlobalSecondaryIndexes: [{
			IndexName: "groupId-index",
			KeySchema: [
				{
					AttributeName: "groupId",
					KeyType: "HASH"
				}
			],
			Projection: {
				ProjectionType: "ALL"
			},
			ProvisionedThroughput: {
				ReadCapacityUnits: 1,
				WriteCapacityUnits: 1
			}
		}]
	},
	{
		TableName: "prefix.books",
		KeySchema: [{
			AttributeName: "id",
			KeyType: "HASH",
		}],
		AttributeDefinitions: [
			{
				AttributeName: "id",
				AttributeType: "S"
			}
		],
		ProvisionedThroughput: {
			ReadCapacityUnits: 1,
			WriteCapacityUnits: 1
		}
	}
];

mockDynamoDB.items = {
	"prefix.users": [
		{ id: "0", groupId: "group-1", __iv: 0 },
		{ id: "1", groupId: "group-1", __iv: 30 },
		{ id: "2", groupId: "group-2", __iv: 1 }
	],
	"prefix.books": [
	]
};

for (let i = 0; i < 150; i++) {
	mockDynamoDB.items["prefix.books"].push({ id: i.toString() });
}

let redisServer;
let redisClient;

describe("RepositoryGenerator", () => {

	let dba;

	before(async () => {

		await mockDynamoDB.start();

		console.log("opening redis server...");
		redisServer = new RedisServer({
			port: 15555
		});

		await redisServer.open();

		console.log("opening redis client...");
		redisClient = redis.createClient({
			port: 15555
		});

		await new Promise((resolve, reject) => {
			redisClient.once("connect", resolve);
		});

		dba = new DynamoRedisAccess();
		dba.log = NoLog.instance;
		dba.ddb = mockDynamoDB.ddb;
		dba.redis = redisClient;
		dba.tableNamePrefix = "prefix.";
	});

	after(async () => {

		console.log("closing redis client...");
		await new Promise((resolve, reject) => {
			redisClient.once("end", resolve);
			redisClient.quit();
		});

		console.log("closing redis server...");
		await redisServer.close();

		await mockDynamoDB.stop();
	});


	beforeEach(async () => {
		await redisClient.flushallAsync();
	});

	it("should scan-cached with miss", async () => {

		const users = await dba.scanCachedVersioned("users", "id");

		assert(0 < users.length);
	});

	it("should scan-cached without miss", async () => {

		const first = await dba.scanCachedVersioned("users", "id");
		const second = await dba.scanCachedVersioned("users", "id");

		assert(0 < first.length);
		assert(0 < second.length);
		assert(first.length === second.length);

	});

	it("should scan-cached without miss", async () => {

		assert.deepStrictEqual(await dba.scanCachedVersioned("users", "id"), [
			{ id: "2", __iv: 1, groupId: "group-2" },
			{ id: "1", __iv: 30, groupId: "group-1" },
			{ id: "0", __iv: 0, groupId: "group-1" }
		]);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users!0", 0, -1, "WITHSCORES"), [
			JSON.stringify({ id: "0", groupId: "group-1", __iv: 0 }), "0"
		]);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users!1", 0, -1, "WITHSCORES"), [
			JSON.stringify({ id: "1", groupId: "group-1", __iv: 30 }), "30"
		]);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users!2", 0, -1, "WITHSCORES"), [
			JSON.stringify({ id: "2", groupId: "group-2", __iv: 1 }), "1"
		]);


		assert.deepStrictEqual(await dba.scanCachedVersioned("users", "id"), [
			{ id: "2", __iv: 1, groupId: "group-2" },
			{ id: "1", __iv: 30, groupId: "group-1" },
			{ id: "0", __iv: 0, groupId: "group-1" }
		]);

		assert.deepStrictEqual(
			await redisClient.zrangeAsync("prefix.users!1", 0, -1, "WITHSCORES"), [
				JSON.stringify({ id: "1", groupId: "group-1", __iv: 30 }), "30"
			]);

		assert.deepStrictEqual(
			await redisClient.zaddAsync("prefix.users!1", 29, JSON.stringify({ id: "1", groupId: "group-1", __iv: 29 })),
			1
		);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users!1", 0, -1, "WITHSCORES"), [
			JSON.stringify({ id: "1", groupId: "group-1", __iv: 29 }), "29",
			JSON.stringify({ id: "1", groupId: "group-1", __iv: 30 }), "30",
		]);

		redisClient.zaddAsync(
			"prefix.users!1",
			31,
			JSON.stringify({ id: "1", groupId: "group-1", __iv: 12 })
		);


		await dba.scanCachedVersioned("users", "id");
		await dba.scanCachedVersioned("users", "id");
	});

	it("scan-cached-versioned, update cached versioned", async () => {

		let first;
		assert.deepStrictEqual(first = await dba.scanCachedVersioned("users", "id"), [
			{ id: '2', groupId: 'group-2', __iv: 1 },
			{ id: '1', groupId: 'group-1', __iv: 30 },
			{ id: '0', groupId: 'group-1', __iv: 0 }
		]);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users", 0, -1, "WITHSCORES"), [
			"2", "0",
			"1", "1",
			"0", "2"
		]);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users!1", 0, -1, "WITHSCORES"), [
			JSON.stringify({ id: "1", groupId: "group-1", __iv: 30 }), "30",
		]);

		first[1].test = true;

		await dba.updateCachedVersioned("users", "id", undefined, first[1]);

		assert.deepStrictEqual(await redisClient.zrangeAsync("prefix.users!1", 0, -1, "WITHSCORES"), [
			JSON.stringify({ id: "1", groupId: "group-1", __iv: 30 }), "30",
			JSON.stringify({ id: '1', groupId: 'group-1', __iv: 31, test: true }), "31",
		]);

		assert.deepStrictEqual(await dba.scanCachedVersioned("users", "id"), [
			{ id: '2', groupId: 'group-2', __iv: 1 },
			{ id: '1', groupId: 'group-1', __iv: 31, test: true },
			{ id: '0', groupId: 'group-1', __iv: 0 }
		]);
	});

	// it("should scan-cached with partial miss", async () => {

	// 	const repositoryGenerator = new RepositoryGenerator();
	// 	repositoryGenerator.log = NoLog.instance;
	// 	repositoryGenerator.tableNamePrefix = "prefix.";

	// 	const TestRepository = repositoryGenerator.generate({
	// 		name: "TestRepository",
	// 		tables: {
	// 			users: {
	// 				hash: "id"
	// 			}
	// 		},
	// 		methods: {
	// 			"scanUsers": "scan-cached users",
	// 		}
	// 	}).value;

	// 	const testRepository = new TestRepository();
	// 	testRepository.log = NoLog.instance;
	// 	testRepository.ddb = mockDynamoDB.ddb;
	// 	testRepository.redis = redisClient;

	// 	const first = await testRepository.scanUsers();

	// 	const ids = await redisClient.zrangeAsync("prefix.users", 0, -1);

	// 	//console.log(await redisClient.keysAsync("*"));

	// 	if (0 < ids.length) {
	// 		const id = "prefix.users!" + ids[Math.floor(Math.random() * ids.length)];
	// 		//console.log("del %j", id);
	// 		await redisClient.delAsync(id);
	// 	}

	// 	//console.log(await redisClient.keysAsync("*"));

	// 	const second = await testRepository.scanUsers();

	// 	assert(0 < first.length);
	// 	assert(0 < second.length);
	// 	assert(first.length === second.length);
	// });
});
