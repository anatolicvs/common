"use strict";
const assert = require('assert');
const redis = require("redis");
const RedisServer = require('redis-server');
const bluebird = require("bluebird");

const { NoLog, ConsoleLog, tools, RepositoryGenerator } = require("..");
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
		{
			id: "user@company",
			groupId: "group@company"
		},
		{
			id: "another.user@company",
			groupId: "group@company"
		},
		{
			id: "yet.another.user@company",
			groupId: "another.group@company"
		},
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

	before(mockDynamoDB.start.bind(mockDynamoDB));

	before(async () => {

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
	});


	after(async () => {

		await new Promise((resolve, reject) => {
			redisClient.once("end", resolve);
			redisClient.quit();
		});

		await redisServer.close();
	});

	after(mockDynamoDB.stop.bind(mockDynamoDB));

	it("should get", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id"
				}
			},
			methods: {
				getUser: "get users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;

		const user = await testRepository.getUser(
			"user@company"
		);

		assert(tools.isObject(user));
		assert(user.id === "user@company");
		assert(user.groupId === "group@company");
	});

	it("should return undefined on get with non-existing key", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id"
				}
			},
			methods: {
				getUser: "get users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;

		const user = await testRepository.getUser(
			"non.existing.user@company"
		);

		assert(user === undefined);
	});

	it("should scan", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					"hash": "id"
				}
			},
			methods: {
				scanUsers: "scan users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;

		const users = await testRepository.scanUsers();

		assert(tools.isArray(users));
		assert(users.length === 3);

		for (const user of users) {

			assert(tools.isObject(user));

			switch (user.id) {
				case "user@company":
					assert(user.groupId === "group@company");
					break;

				case "another.user@company":
					assert(user.groupId === "group@company");
					break;

				case "yet.another.user@company":
					assert(user.groupId === "another.group@company");
					break;

				default:
					assert(false);
			}
		}
	});

	it("should get-cached with miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id"
				}
			},
			methods: {
				getUser: "get-cached users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		const user = await testRepository.getUser("user@company");

		assert(user.id === "user@company");
	});

	it("should get-cached without miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			"name": "TestRepository",
			"tables": {
				"users": {
					"hash": "id"
				}
			},
			"methods": {
				"getUser": "get-cached users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		const first = await testRepository.getUser("user@company");
		const second = await testRepository.getUser("user@company");

		assert(first.id === "user@company");
		assert(second.id === "user@company");
	});

	it("should scan-cached with miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					"hash": "id"
				}
			},
			methods: {
				scanUsers: "scan-cached users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		const users = await testRepository.scanUsers();
		assert(0 < users.length);
	});

	it("should scan-cached without miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id"
				}
			},
			methods: {
				scanUsers: "scan-cached users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		const first = await testRepository.scanUsers();
		const second = await testRepository.scanUsers();

		assert(0 < first.length);
		assert(0 < second.length);
		assert(first.length === second.length);
	});

	it("should scan-cached with partial miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id"
				}
			},
			methods: {
				"scanUsers": "scan-cached users",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		const first = await testRepository.scanUsers();

		const ids = await redisClient.zrangeAsync("prefix.users", 0, -1);

		//console.log(await redisClient.keysAsync("*"));

		if (0 < ids.length) {
			const id = "prefix.users!" + ids[Math.floor(Math.random() * ids.length)];
			//console.log("del %j", id);
			await redisClient.delAsync(id);
		}

		//console.log(await redisClient.keysAsync("*"));

		const second = await testRepository.scanUsers();

		assert(0 < first.length);
		assert(0 < second.length);
		assert(first.length === second.length);
	});

	it("should query-index", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id",
					indices: {
						"groupId-index": {
							hash: "groupId"
						}
					}
				}
			},
			methods: {
				queryUsersByGroup: "query-index users groupId-index",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;

		const users = await testRepository.queryUsersByGroup("group@company");
		assert(tools.isArray(users));
		assert(users.length === 2);

		const user = users[0];
		assert(tools.isObject(user));
		assert(user.id === "user@company");
		assert(user.groupId === "group@company");

		const anotherUser = users[1];
		assert(tools.isObject(anotherUser));
		assert(anotherUser.id === "another.user@company");
		assert(anotherUser.groupId === "group@company");
	});

	it("should query-index-cached", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id",
					indices: {
						"groupId-index": {
							hash: "groupId"
						}
					}
				}
			},
			methods: {
				queryUsersByGroup: "query-index-cached users groupId-index",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		const first = await testRepository.queryUsersByGroup("group@company");
		const keys = await redisClient.keysAsync("*");

		// for (let i = 0; i < keys.length; i++) {
		// 	const key = keys[i];
		// 	console.log(key, await redisClient.ttlAsync(key));
		// }

		const second = await testRepository.queryUsersByGroup("group@company");
	});

	it("should batch-get", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				books: {
					hash: "id"
				}
			},
			methods: {
				batchGetBooks: "batch-get books",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;

		const ids = [];
		const map = new Map();
		for (let i = 0; i < 150; i++) {

			const bookId = i.toString();
			ids.push(bookId);
			map.set(bookId, null);
		}

		const books = await testRepository.batchGetBooks(
			ids
		);

		assert(tools.isArray(books));
		assert(books.length === 150);

		for (const book of books) {

			const bookId = book.id;
			assert(map.has(bookId));
			assert(map.get(bookId) === null);
			map.set(bookId, book);
		}
	});

	it("should batch-get-cached full miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				books: {
					hash: "id"
				}
			},
			methods: {
				batchGetBooks: "batch-get-cached books",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		await redisClient.flushallAsync();
		const keysBefore = await redisClient.keysAsync("*");
		assert(tools.isArray(keysBefore));
		assert(keysBefore.length === 0);

		const ids = ["0", "1", "2"];
		const map = new Map();
		for (const id of ids) {
			map.set(id, null);
		}

		const books = await testRepository.batchGetBooks(
			ids
		);

		assert(tools.isArray(books));
		assert(books.length === 3);

		for (const book of books) {

			const bookId = book.id;
			assert(map.has(bookId));
			assert(map.get(bookId) === null);
			map.set(bookId, book);
		}

		const keysAfter = await redisClient.keysAsync("*");
		assert(tools.isArray(keysAfter));
		assert(keysAfter.length === 3);
		for (const key of keysAfter) {

			const bookId = key.substr("prefix.books!".length, key.length);
			assert(map.has(bookId));
			assert(map.get(bookId) !== null);
		}
	});

	it("should batch-get-cached partial miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				books: {
					hash: "id"
				}
			},
			methods: {
				batchGetBooks: "batch-get-cached books",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		await redisClient.flushallAsync();
		const keysBefore = await redisClient.keysAsync("*");
		assert(tools.isArray(keysBefore));
		assert(keysBefore.length === 0);

		await redisClient.setAsync("prefix.books!0", JSON.stringify({ id: "0" }));

		const partialKeys = await redisClient.keysAsync("*");
		assert(tools.isArray(partialKeys));
		assert(partialKeys.length === 1);
		assert(partialKeys[0] === "prefix.books!0");

		const ids = ["0", "1", "2"];
		const map = new Map();
		for (const id of ids) {
			map.set(id, null);
		}

		const books = await testRepository.batchGetBooks(
			ids
		);

		assert(tools.isArray(books));
		assert(books.length === 3);

		for (const book of books) {

			const bookId = book.id;
			assert(map.has(bookId));
			assert(map.get(bookId) === null);
			map.set(bookId, book);
		}

		const keysAfter = await redisClient.keysAsync("*");
		assert(tools.isArray(keysAfter));
		assert(keysAfter.length === 3);
		for (const key of keysAfter) {

			const bookId = key.substr("prefix.books!".length, key.length);
			assert(map.has(bookId));
			assert(map.get(bookId) !== null);
		}
	});

	it("should batch-get-cached no miss", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				books: {
					hash: "id"
				}
			},
			methods: {
				batchGetBooks: "batch-get-cached books",
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;
		testRepository.redis = redisClient;

		await redisClient.flushallAsync();
		const keysBefore = await redisClient.keysAsync("*");
		assert(tools.isArray(keysBefore));
		assert(keysBefore.length === 0);

		const ids = ["0", "1", "2"];
		const map = new Map();
		for (const id of ids) {
			map.set(id, null);
		}

		const books = await testRepository.batchGetBooks(
			ids
		);

		assert(tools.isArray(books));
		assert(books.length === 3);

		for (const book of books) {

			const bookId = book.id;
			assert(map.has(bookId));
			assert(map.get(bookId) === null);
			map.set(bookId, book);
		}

		const keysAfter = await redisClient.keysAsync("*");
		assert(tools.isArray(keysAfter));
		assert(keysAfter.length === 3);
		for (const key of keysAfter) {

			const bookId = key.substr("prefix.books!".length, key.length);
			assert(map.has(bookId));
			assert(map.get(bookId) !== null);
		}

		const booksAgain = await testRepository.batchGetBooks(
			ids
		);

		assert(tools.isArray(booksAgain));
		assert(booksAgain.length === 3);

		for (const book of booksAgain) {

			const bookId = book.id;
			assert(map.has(bookId));
			assert(map.get(bookId).id === bookId);
		}
	});

	it("should handle update-versioned", async () => {

		const repositoryGenerator = new RepositoryGenerator();
		repositoryGenerator.log = NoLog.instance;
		repositoryGenerator.tableNamePrefix = "prefix.";

		const TestRepository = repositoryGenerator.generate({
			name: "TestRepository",
			tables: {
				users: {
					hash: "id"
				}
			},
			methods: {
				getUser: "get users",
				updateUser: "update-versioned users"
			}
		}).value;

		const testRepository = new TestRepository();
		testRepository.log = NoLog.instance;
		testRepository.ddb = mockDynamoDB.ddb;

		let throws;
		try {
			await testRepository.updateUser({
				id: "null",
				groupId: "group@company"
			});
		}
		catch (error) {
			if (error.code === "ConditionalCheckFailedException") {
				throws = true;
			}
			else {
				throw error;
			}
		}

		assert(throws);


		const user1 = await testRepository.getUser(
			"user@company"
		);

		const user2 = await testRepository.getUser(
			"user@company"
		);

		assert(user1["--iv"] === undefined);
		assert(user2["--iv"] === undefined);

		user1.counter = 5;

		await testRepository.updateUser(
			user1
		);

		assert(user1["--iv"] === 0);
		assert(user2["--iv"] === undefined);

		user2.counter = 6;

		throws = undefined;
		try {
			await testRepository.updateUser(
				user2
			);
		}
		catch (error) {
			if (error.code === "ConditionalCheckFailedException") {
				throws = true;
			}
			else {
				throw error;
			}
		}

		assert(throws);

		assert(user1["--iv"] === 0);
		assert(user2["--iv"] === undefined);

		user1.counter++;

		await testRepository.updateUser(
			user1
		);

		assert(user1["--iv"] === 1);
		assert(user2["--iv"] === undefined);

		user1.counter++;

		await testRepository.updateUser(
			user1
		);

		assert(user1["--iv"] === 2);
		assert(user2["--iv"] === undefined);

		delete user1.counter;

		await testRepository.updateUser(
			user1
		);

		assert(user1["--iv"] === 3);
		assert(user2["--iv"] === undefined);

		const user2retry = await testRepository.getUser(
			"user@company"
		);

		assert(user1["--iv"] === 3);
		assert(user2["--iv"] === undefined);
		assert(user2retry["--iv"] === 3);

		user2retry.x = "234";

		await testRepository.updateUser(
			user2retry
		);

		assert(user1["--iv"] === 3);
		assert(user2["--iv"] === undefined);
		assert(user2retry["--iv"] === 4);


		user1.y = 123;

		throws = undefined;
		try {
			await testRepository.updateUser(
				user1
			);
		}
		catch (error) {
			if (error.code === "ConditionalCheckFailedException") {
				throws = true;
			}
			else {
				throw error;
			}
		}

		assert(throws);

		assert(user1["--iv"] === 3);
		assert(user2["--iv"] === undefined);
		assert(user2retry["--iv"] === 4);
	});
});
