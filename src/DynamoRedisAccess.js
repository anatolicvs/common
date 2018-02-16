"use strict";

const kItemVersionKey = "__iv";

function assertNonEmptyString(value) {

	if (typeof value === "string") {
		// ok
	}
	else {
		throw new Error();
	}

	if (0 < value.length) {
		// ok
	}
	else {
		throw new Error();
	}
}

function assertOptionalNonEmptyString(value) {

	if (value === undefined) {
		return;
	}

	assertNonEmptyString(
		value
	);
}

class DynamoRedisAccess {

	async create(tableName, hash, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ConditionExpression: `attribute_not_exists(${hash})`,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"create %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"create %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async createOrGet(tableName, hash, range, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);
		assertOptionalNonEmptyString(range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let existingItem;
		let consumed = 0;

		const time = process.hrtime();

		try {

			let putResponse;
			try {

				putResponse = await this.ddb.put({
					TableName: prefixedTableName,
					Item: item,
					ConditionExpression: `attribute_not_exists(${hash})`,
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				consumed = putResponse.ConsumedCapacity.CapacityUnits;

				return;
			}
			catch (error) {

				if (error.code !== "ConditionalCheckFailedException") {
					throw error;
				}
			}

			const getResponse = await this.ddb.get({
				TableName: prefixedTableName,
				Key: range === undefined ? { [hash]: item[hash] } : { [hash]: item[hash], [range]: item[range] },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			existingItem = getResponse.Item;
			consumed = getResponse.ConsumedCapacity.CapacityUnits;

			return existingItem;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			this.log.debug(
				"create-or-get %s %d %d %s",
				prefixedTableName,
				existingItem === undefined ? 0 : 1,
				consumed,
				elapsed
			);
		}
	}

	async update(tableName, hash, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ConditionExpression: `attribute_exists(${hash})`,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"update %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"update-versioned %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async updateCachedVersioned(tableName, hash, range, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);

		const iv = item[kItemVersionKey];

		if (Number.isFinite(iv)) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;
		const ttl = 600;
		const nextiv = Math.floor(iv) + 1;

		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: { ...item, [kItemVersionKey]: nextiv },
				ConditionExpression: "#iv = :iv",
				ExpressionAttributeNames: {
					"#iv": kItemVersionKey
				},
				ExpressionAttributeValues: {
					":iv": iv
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item[kItemVersionKey] = nextiv;

			consumed = response.ConsumedCapacity.CapacityUnits;

			if (this.redis.connected) {

				try {

					let key;
					if (range === undefined) {
						key = `${prefixedTableName}!${item[hash]}`;
					}
					else {
						key = `${prefixedTableName}!${item[hash]}!${item[range]}`;
					}

					const json = JSON.stringify(
						item
					);

					const multi = this.redis.multi();

					multi.zadd(
						key,
						nextiv,
						json
					);

					multi.expire(
						key,
						ttl
					);

					await multi.execAsync();
				}
				catch (error) {

					this.log.warn(
						error
					)
				}
			}
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"update-cached-versioned %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"update-cached-versioned %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async updateVersioned(tableName, hash, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const iv = item[kItemVersionKey];

			if (Number.isFinite(iv)) {

				const nextiv = Math.floor(iv) + 1;

				const response = await this.ddb.put({
					TableName: prefixedTableName,
					Item: { ...item, [kItemVersionKey]: nextiv },
					ConditionExpression: "#iv = :iv",
					ExpressionAttributeNames: {
						"#iv": kItemVersionKey
					},
					ExpressionAttributeValues: {
						":iv": iv
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				item[kItemVersionKey] = nextiv;

				consumed = response.ConsumedCapacity.CapacityUnits;
			}
			else {

				const response = await this.ddb.put({
					TableName: prefixedTableName,
					Item: { ...item, [kItemVersionKey]: 0 },
					ConditionExpression: `attribute_exists(${hash}) and attribute_not_exists(#iv)`,
					ExpressionAttributeNames: {
						"#iv": kItemVersionKey
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				item[kItemVersionKey] = 0;

				consumed = response.ConsumedCapacity.CapacityUnits;
			}
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"update-versioned %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"update-versioned %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async put(tableName, item) {

		assertNonEmptyString(tableName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"put %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"put %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async delete(tableName, hash, range, hashValue, rangeValue) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);
		assertOptionalNonEmptyString(range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: range === undefined ? { [hash]: hashValue } : { [hash]: hashValue, [range]: rangeValue },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"delete %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"put %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async get(tableName, hash, range, hashValue, rangeValue) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);
		assertOptionalNonEmptyString(range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			const response = await this.ddb.get({
				TableName: prefixedTableName,
				Key: range === undefined ? { [hash]: hashValue } : { [hash]: hashValue, [range]: rangeValue },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const item = response.Item;

			length = item === undefined ? 0 : 1;
			consumed = response.ConsumedCapacity.CapacityUnits;

			return item;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"get %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"get %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async scan(tableName) {

		assertNonEmptyString(tableName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;

		const time = process.hrtime();

		try {

			const response = await this.ddb.scan({
				TableName: prefixedTableName,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;
			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			return items;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			this.log.debug(
				"scan %s %d %d %s",
				prefixedTableName,
				length,
				consumed,
				elapsed
			);
		}
	}

	async scanCachedVersioned(tableName, hash, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);
		assertOptionalNonEmptyString(range);

		const prefixedTableName = this.tableNamePrefix + tableName;
		const ttl = 600;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = process.hrtime();

		try {

			if (this.redis.connected) {

				try {

					const ids = await this.redis.zrangeAsync(
						prefixedTableName,
						0,
						-1
					);

					length = ids.length;

					if (0 < length) {

						const multi = this.redis.multi();

						for (let i = 0; i < length; i++) {

							const id = ids[i];
							const key = `${prefixedTableName}!${id}`;

							multi.zrange(
								key,
								-1,
								-1,
								"WITHSCORES"
							);
						}

						const jsons = await multi.execAsync();

						let miss;
						let result = [];

						for (let i = 0; i < length; i++) {

							const json = jsons[i];

							if (Array.isArray(json) && json.length === 2) {

								const item = JSON.parse(
									json[0]
								);

								const score = Number.parseFloat(
									json[1]
								);

								if (item[kItemVersionKey] === score) {
									// ok
								}
								else {

									miss = true;
									break;
								}

								result.push(
									item
								);
							}
							else {

								miss = true;
								break;
							}
						}

						if (miss === true) {
							// ok
						}
						else {
							return result;
						}
					}
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			const response = await this.ddb.scan({
				TableName: prefixedTableName,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;
			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();
						const ids = [];

						for (let i = 0; i < length; i++) {

							const item = items[i];

							const iv = item[kItemVersionKey];

							if (!Number.isFinite(iv)) {
								throw new Error();
							}

							// id is `${hash}` or `${hash}!${range}`

							let id;
							if (range === undefined) {
								id = item[hash];
							}
							else {
								id = `${item[hash]}!${item[range]}`;
							}

							const key = `${prefixedTableName}!${id}`;
							const json = JSON.stringify(
								item
							);

							multi.zadd(
								key,
								iv,
								json
							);

							multi.expire(
								key,
								ttl
							);

							ids.push(i, id);
						}

						multi.del(
							prefixedTableName
						);

						multi.zadd(
							prefixedTableName,
							ids
						);

						multi.expire(
							prefixedTableName,
							ttl
						);

						// no need to wait
						await multi.execAsync();
					}
					catch (error) {

						this.log.warn(
							error
						)
					}
				}
			}

			return items;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.debug(
					"scan-cached %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.debug(
					"scan-cached %s %j %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async enumerateTable(tableName) {

		assertNonEmptyString(tableName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let ExclusiveStartKey = null;
		do {

			const response = await this.ddb.scan({
				TableName: prefixedTableName,
				ExclusiveStartKey,
				Limit: limit,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;
			const lastEvaluatedKey = response.LastEvaluatedKey;

			if (await iterator(items) === true) {
				break;
			}

			ExclusiveStartKey = lastEvaluatedKey;

		} while (ExclusiveStartKey);
	}

	async batchGet(tableName, hash, range, hashes) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);
		assertOptionalNonEmptyString(range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length;
		let consumed = 0;
		const time = process.hrtime();
		try {
			const queue = [];
			const map = new Map();
			const results = [];

			for (const hashValue of hashes) {

				if (map.has(hashValue)) {

					this.log.warn(
						"%j is duplicate.",
						hashValue
					);
				}
				else {

					queue.push(
						hashValue
					);

					map.set(hashValue, null);
				}
			}

			do {

				// dequeue 100 ids from queue
				const chunk = queue.splice(0, 100);

				// prepare keys
				const keys = [];
				for (const hashValue of chunk) {

					keys.push({
						[hash]: hashValue
					});

				}

				this.log.debug(
					"batch get %d id(s)...",
					chunk.length
				);

				// batch get
				const response = await this.ddb.batchGet({
					RequestItems: {
						[prefixedTableName]: {
							Keys: keys
						}
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				// accumulate consumed capacity units
				consumed += response.ConsumedCapacity[0].CapacityUnits;

				// get items
				const items = response.Responses[prefixedTableName];
				if (0 < items.length) {

					this.log.debug("got %d item(s).", items.length);
					for (const item of items) {

						const hashValue = item[hash];
						const value = map.get(
							hashValue
						);

						if (value === undefined) {

							this.log.error("value not found.");
							throw new Error();
						}
						else if (value === null) {
							map.set(hashValue, item);
						}
						else {
							this.log.error("value already in map.");
							throw new Error();
						}

						results.push(
							item
						);
					}
				}

				const unprocessedKeys = response.UnprocessedKeys[prefixedTableName];

				if (unprocessedKeys === undefined) {
					// ok
				}
				else {

					const keys = unprocessedKeys.Keys;

					this.log.debug(
						"%d id(s) are unprocessed.",
						keys.length
					);

					for (const key of keys) {

						const hashValue = key[hash];
						const value = map.get(
							hashValue
						);

						// check (optional)
						if (value === void 0) {
							this.log.error("value not found.");
							throw new Error();
						}
						else if (value === null) {
						}
						else {
							this.log.error("value already in map.");
							throw new Error();
						}
						queue.push(hash);
					}
				}
			} while (0 < queue.length);
			length = results.length;
			return results;
		}
		finally {
			const diff = process.hrtime(time);
			const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);
			lines.push(`		this.log.debug("'${methodName}' batch-get ${prefixedTableName} %d %d %s", length, consumed, elapsed);`);
		}
	}
}

DynamoRedisAccess.prototype.log = null;
DynamoRedisAccess.prototype.ddb = null;
DynamoRedisAccess.prototype.redis = null;
DynamoRedisAccess.prototype.tableNamePrefix = null;

module.exports = {
	DynamoRedisAccess
};
