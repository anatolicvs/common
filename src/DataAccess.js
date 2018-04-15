"use strict";

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

function assertPositiveInteger(value) {

	if (Number.isInteger(value)) {
		// ok
	}
	else {
		throw new Error();
	}

	if (0 < value) {
		// ok
	}
	else {
		throw new Error();
	}
}

const {
	hrtime
} = process;

class DataAccess {

	async getCachedItems(prefixedTableName, setKey) {

		if (this.redis.connected) {

			try {

				const ids = await this.redis.zrangeAsync(
					setKey,
					0,
					-1
				);

				if (0 < ids.length) {

					const multi = this.redis.multi();

					for (const id of ids) {

						const key = `${prefixedTableName}!${id}`;

						multi.get(
							key
						);
					}

					const jsons = await multi.execAsync();

					if (jsons.indexOf(null) < 0) {
						return jsons.map(JSON.parse);
					}
				}
			}
			catch (error) {

				this.log.warn(
					error
				);
			}
		}
	}

	async create(tableName, hashName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ConditionExpression: "attribute_not_exists(#hash)",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"create %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"create %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async createVersioned(tableName, hashName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(versionName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: { ...item, [versionName]: 0 },
				ConditionExpression: `attribute_not_exists(#hash)`,
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item[versionName] = 0;
			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"create-versioned %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"create-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async createCachedVersioned(ttl, tableName, hashName, rangeName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(versionName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: { ...item, [versionName]: 0 },
				ConditionExpression: `attribute_not_exists(#hash)`,
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item[versionName] = 0;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (this.redis.connected) {

				try {

					let key;
					if (rangeName === undefined) {
						key = `${prefixedTableName}!${item[hashName]}`;
					}
					else {
						key = `${prefixedTableName}!${item[hashName]}!${item[rangeName]}`;
					}

					const json = JSON.stringify(
						item
					);

					const multi = this.redis.multi();

					multi.del(
						key
					);

					multi.zadd(
						key,
						0,
						json
					);

					multi.expire(
						key,
						ttl
					);

					// no need to wait
					multi.exec((error, reply) => {

						if (error) {
							this.log.warn(
								error
							);
						}
					});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"create-cached-versioned %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"create-cached-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async createOrGet(tableName, hashName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let existingItem;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			try {

				const putResponse = await this.ddb.put({
					TableName: prefixedTableName,
					Item: item,
					ConditionExpression: "attribute_not_exists(#hash)",
					ExpressionAttributeNames: {
						"#hash": hashName
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				consumed = putResponse.ConsumedCapacity.CapacityUnits;

				return;
			}
			catch (error) {

				if (error.code === "ConditionalCheckFailedException") {
					// ok
				}
				else {
					throw error;
				}
			}

			const getResponse = await this.ddb.get({
				TableName: prefixedTableName,
				Key: { [hashName]: item[hashName] },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			existingItem = getResponse.Item;
			consumed = getResponse.ConsumedCapacity.CapacityUnits;

			return existingItem;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"create-or-get %s(%s) %d %d %s",
					prefixedTableName,
					hashName,
					existingItem === undefined ? 0 : 1,
					consumed,
					elapsed
				);
			}
			else {
				this.log.warn(
					"create-or-get %s(%s) %s %s",
					prefixedTableName,
					hashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async createOrGetRanged(tableName, hashName, rangeName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let existingItem;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			try {

				const putResponse = await this.ddb.put({
					TableName: prefixedTableName,
					Item: item,
					ConditionExpression: "attribute_not_exists(#hash)",
					ExpressionAttributeNames: {
						"#hash": hashName
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				consumed = putResponse.ConsumedCapacity.CapacityUnits;

				return;
			}
			catch (error) {

				if (error.code === "ConditionalCheckFailedException") {
					// ok
				}
				else {
					throw error;
				}
			}

			const getResponse = await this.ddb.get({
				TableName: prefixedTableName,
				Key: { [hashName]: item[hashName], [rangeName]: item[rangeName] },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			existingItem = getResponse.Item;
			consumed = getResponse.ConsumedCapacity.CapacityUnits;

			return existingItem;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"create-or-get %s(%s,%s) %d %d %s",
					prefixedTableName,
					hashName,
					rangeName,
					existingItem === undefined ? 0 : 1,
					consumed,
					elapsed
				);
			}
			else {
				this.log.warn(
					"create-or-get %s(%s,%s) %s %s",
					prefixedTableName,
					hashName,
					rangeName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async getOrCreate(tableName, hashName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let existingItem;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const getResponse = await this.ddb.get({
				TableName: prefixedTableName,
				Key: { [hashName]: item[hashName] },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			existingItem = getResponse.Item;
			consumed = getResponse.ConsumedCapacity.CapacityUnits;

			if (existingItem !== undefined) {
				return existingItem;
			}

			const putResponse = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ConditionExpression: "attribute_not_exists(#hash)",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed += putResponse.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"get-or-create %s(%s) %d %d %s",
					prefixedTableName,
					hashName,
					existingItem === undefined ? 0 : 1,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"get-or-create %s(%s) %s %s",
					prefixedTableName,
					hashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async getOrCreateRanged(tableName, hashName, rangeName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let existingItem;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const getResponse = await this.ddb.get({
				TableName: prefixedTableName,
				Key: { [hashName]: item[hashName], [rangeName]: item[rangeName] },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			existingItem = getResponse.Item;
			consumed = getResponse.ConsumedCapacity.CapacityUnits;

			if (existingItem !== undefined) {
				return existingItem;
			}

			const putResponse = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ConditionExpression: "attribute_not_exists(#hash)",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed += putResponse.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"get-or-create %s(%s,%s) %d %d %s",
					prefixedTableName,
					hashName,
					rangeName,
					existingItem === undefined ? 0 : 1,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"get-or-create %s(%s,%s) %s %s",
					prefixedTableName,
					hashName,
					rangeName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async update(tableName, hashName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: item,
				ConditionExpression: "attribute_exists(#hash)",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"update %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"update %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async updateCachedVersioned(ttl, tableName, hashName, rangeName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(hashName);
		assertNonEmptyString(versionName);

		const version = item[versionName];

		if (Number.isFinite(version)) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;
		const nextVersion = Math.floor(version) + 1;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: { ...item, [versionName]: nextVersion },
				ConditionExpression: "#version = :version",
				ExpressionAttributeNames: {
					"#version": versionName
				},
				ExpressionAttributeValues: {
					":version": version
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item[versionName] = nextVersion;

			consumed = response.ConsumedCapacity.CapacityUnits;

			if (this.redis.connected) {

				try {

					let key;
					if (rangeName === undefined) {
						key = `${prefixedTableName}!${item[hashName]}`;
					}
					else {
						key = `${prefixedTableName}!${item[hashName]}!${item[rangeName]}`;
					}

					const json = JSON.stringify(
						item
					);

					const multi = this.redis.multi();

					multi.zadd(
						key,
						nextVersion,
						json
					);

					multi.expire(
						key,
						ttl
					);

					// no need to wait
					multi.exec((error, reply) => {

						if (error) {
							this.log.warn(
								error
							);
						}
					});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"update-cached-versioned %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"update-cached-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async deleteCached(tableName, hashName, hash, rangeName, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let key;
		if (rangeName === undefined) {
			key = `${prefixedTableName}!${hash}`;
		}
		else {
			key = `${prefixedTableName}!${hash}!${range}`;
		}

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;

			if (this.redis.connected) {

				try {

					await this.redis.delAsync(
						key
					);
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"delete-cached %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"delete-cached %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async removeVersioned(tableName, hashName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		const hash = item[hashName];
		const version = item[versionName];

		if (Number.isFinite(version)) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: { [hashName]: hash },
				ConditionExpression: "#version = :version",
				ExpressionAttributeNames: {
					"#version": versionName
				},
				ExpressionAttributeValues: {
					":version": version
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"remove-versioned %s(%s,%s) %d %s",
					prefixedTableName,
					hashName,
					versionName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"remove-versioned %s(%s,%s) %s %s",
					prefixedTableName,
					hashName,
					versionName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async removeRangedVersioned(tableName, hashName, rangeName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(rangeName);

		const hash = item[hashName];
		const range = item[rangeName];
		const version = item[versionName];

		if (Number.isFinite(version)) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: { [hashName]: hash, [rangeName]: range },
				ConditionExpression: "#version = :version",
				ExpressionAttributeNames: {
					"#version": versionName
				},
				ExpressionAttributeValues: {
					":version": version
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"remove-versioned %s(%s,%s,%s) %d %s",
					prefixedTableName,
					hashName,
					rangeName,
					versionName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"remove-versioned %s(%s,%s,%s) %s %s",
					prefixedTableName,
					hashName,
					rangeName,
					versionName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async removeCachedVersioned(tableName, hashName, rangeName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);

		const hash = item[hashName];

		const version = item[versionName];
		if (Number.isFinite(version)) {
			// ok
		}
		else {
			throw new Error();
		}

		let range;
		if (rangeName === undefined) {
			// ok
		}
		else {
			range = item[rangeName];
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		let key;
		if (rangeName === undefined) {
			key = `${prefixedTableName}!${hash}`;
		}
		else {
			key = `${prefixedTableName}!${hash}!${range}`;
		}

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ConditionExpression: "#version = :version",
				ExpressionAttributeNames: {
					"#version": versionName
				},
				ExpressionAttributeValues: {
					":version": version
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;

			if (this.redis.connected) {

				try {

					await this.redis.delAsync(
						key
					);
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"remove-cached-versioned %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"remove-cached-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async updateVersioned(tableName, versionName, item) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(versionName);

		const version = item[versionName];

		if (Number.isFinite(version)) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;
		const nextVersion = Math.floor(version) + 1;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.put({
				TableName: prefixedTableName,
				Item: { ...item, [versionName]: nextVersion },
				ConditionExpression: "#version = :version",
				ExpressionAttributeNames: {
					"#version": versionName
				},
				ExpressionAttributeValues: {
					":version": version
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item[versionName] = nextVersion;

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"update-versioned %s(%d->%d) %d %s",
					prefixedTableName,
					version,
					nextVersion,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"update-versioned %s(%d->%d) %s %s",
					prefixedTableName,
					version,
					nextVersion,
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

		const time = hrtime();

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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"put %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"put %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async delete(tableName, hashName, rangeName, hash, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"delete %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"delete %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async remove(tableName, hashName, hash, rangeName, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.delete({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ConditionExpression: "attribute_exists(#hash)",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			consumed = response.ConsumedCapacity.CapacityUnits;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"remove %s %d %s",
					prefixedTableName,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"remove %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async get(tableName, hashName, hash, rangeName, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.get({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"get %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"get %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async getCached(ttl, tableName, hashName, hash, rangeName, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let key;
		if (rangeName === undefined) {
			key = `${prefixedTableName}!${hash}`;
		}
		else {
			key = `${prefixedTableName}!${hash}!${range}`;
		}

		let item;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			if (this.redis.connected) {

				try {

					const json = await this.redis.getAsync(
						key
					);

					if (json === null) {
						// ok
					}
					else {

						item = JSON.parse(
							json
						);

						return item;
					}
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			const response = await this.ddb.get({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item = response.Item;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (item === undefined) {
				return;
			}

			if (this.redis.connected) {

				try {

					const json = JSON.stringify(
						item
					);

					this.redis.set(key, json, "EX", ttl, (error, reply) => {

						if (error) {
							this.log.warn(
								error
							);
						}

					});
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			return item;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"get-cached %s %d %d %s",
					prefixedTableName,
					item === undefined ? 0 : 1,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"get-cached %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async getCachedVersioned(ttl, tableName, hashName, hash, versionName, rangeName, range) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(versionName);
		assertOptionalNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let key;
		if (rangeName === undefined) {
			key = `${prefixedTableName}!${hash}`;
		}
		else {
			key = `${prefixedTableName}!${hash}!${range}`;
		}

		let item;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			if (this.redis.connected) {

				try {

					const result = await this.redis.zrangeAsync(
						key,
						-1,
						-1,
						"WITHSCORES"
					);

					if (result.length === 2) {

						const json = result[0];
						const scoreString = result[1];

						item = JSON.parse(
							json
						);

						const score = Number.parseFloat(
							scoreString
						);

						if (item[versionName] === score) {
							return item;
						}
					}
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			const response = await this.ddb.get({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			item = response.Item;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (item === undefined) {
				return;
			}

			if (this.redis.connected) {

				try {

					const iv = item[versionName];
					if (Number.isFinite(iv)) {

						const json = JSON.stringify(
							item
						);

						//await this.redis.setAsync(key, json, "EX", ttl);

						const multi = this.redis.multi();

						multi.zadd(
							key,
							iv,
							json
						);

						multi.expire(
							key,
							ttl
						);

						// no need to wait
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
					}
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			return item;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"get-cached-versioned %s %d %d %s",
					prefixedTableName,
					item === undefined ? 0 : 1,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"get-cached-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async getConsistent(tableName, hashName, hash, rangeName, range) {

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.get({
				TableName: prefixedTableName,
				Key: rangeName === undefined ? { [hashName]: hash } : { [hashName]: hash, [rangeName]: range },
				ConsistentRead: true,
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"get-consistent %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"get-consistent %s %s %s",
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
		let caught;

		const time = hrtime();

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
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"scan %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"scan %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async scanCached(ttl, tableName, hashName) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			let items = await this.getCachedItems(
				prefixedTableName,
				prefixedTableName
			);

			if (items === undefined) {
				// ok
			}
			else {
				length = items.length;
				return items;
			}

			const response = await this.ddb.scan({
				TableName: prefixedTableName,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			items = response.Items;
			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();
						const ids = [];

						for (let i = 0; i < length; i++) {

							const item = items[i];

							const id = item[hashName];
							const key = `${prefixedTableName}!${id}`;

							const json = JSON.stringify(
								item
							);

							multi.set(
								key,
								json,
								"EX",
								ttl
							);

							ids.push(
								i,
								id
							);
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
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"scan-cached %s(%s) %d %d %s",
					prefixedTableName,
					hashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"scan-cached %s(%s) %s %s",
					prefixedTableName,
					hashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async scanRangedCached(ttl, tableName, hashName, rangeName) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			let items = await this.getCachedItems(
				prefixedTableName,
				prefixedTableName
			);

			if (items === undefined) {
				// ok
			}
			else {
				length = items.length;
				return items;
			}

			const response = await this.ddb.scan({
				TableName: prefixedTableName,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			items = response.Items;
			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();
						const ids = [];

						for (let i = 0; i < length; i++) {

							const item = items[i];

							const id = `${item[hashName]}!${item[rangeName]}`;
							const key = `${prefixedTableName}!${id}`;

							const json = JSON.stringify(
								item
							);

							multi.set(
								key,
								json,
								"EX",
								ttl
							);

							ids.push(
								i,
								id
							);
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
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"scan-cached %s(%s,%s) %d %d %s",
					prefixedTableName,
					hashName,
					rangeName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"scan-cached %s(%s,%s) %s %s",
					prefixedTableName,
					hashName,
					rangeName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async scanCachedVersioned(ttl, tableName, hashName, rangeName, versionName) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);
		assertNonEmptyString(versionName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

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

								if (item[versionName] === score) {
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

							const version = item[versionName];

							if (!Number.isFinite(version)) {
								throw new Error();
							}

							// id is `${hash}` or `${hash}!${range}`

							let id;
							if (rangeName === undefined) {
								id = item[hashName];
							}
							else {
								id = `${item[hashName]}!${item[rangeName]}`;
							}

							const key = `${prefixedTableName}!${id}`;

							const json = JSON.stringify(
								item
							);

							multi.zadd(
								key,
								version,
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
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"scan-cached-versioned %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"scan-cached-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async query(params) {

		const {
			TableName: tableName,
			ReturnConsumedCapacity: returnConsumedCapacity
		} = params;

		const prefixedTableName = this.tableNamePrefix + tableName;

		params.TableName = prefixedTableName;

		if (returnConsumedCapacity === undefined) {
			params.ReturnConsumedCapacity = "TOTAL";
		}

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.query(
				params
			).promise();

			const items = response.Items;

			length = items.length;

			if (returnConsumedCapacity === undefined) {
				consumed = response.ConsumedCapacity.CapacityUnits;
			}

			return response;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryTable(tableName, hashName, hash) {

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				KeyConditionExpression: "#hash = :hash",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ExpressionAttributeValues: {
					":hash": hash
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			return items;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-table %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-table %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryTableRangedCached(ttl, tableName, hashName, rangeName, hash) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(rangeName);

		const prefixedTableName = this.tableNamePrefix + tableName;
		const setKey = `${prefixedTableName}!${hash}`;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			let items = await this.getCachedItems(
				prefixedTableName,
				setKey
			);

			if (items === undefined) {
				// ok
			}
			else {
				length = items.length;
				return items;
			}

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				KeyConditionExpression: "#hash = :hash",
				ExpressionAttributeNames: {
					"#hash": hashName
				},
				ExpressionAttributeValues: {
					":hash": hash
				},
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			items = response.Items;
			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();
						const ids = [];

						for (let i = 0; i < length; i++) {

							const item = items[i];

							const id = `${item[hashName]}!${item[rangeName]}`;
							const key = `${prefixedTableName}!${id}`;

							const json = JSON.stringify(
								item
							);

							multi.set(
								key,
								json,
								"EX",
								ttl
							);

							ids.push(
								i,
								id
							);
						}

						multi.del(
							setKey
						);

						multi.zadd(
							setKey,
							ids
						);

						multi.expire(
							setKey,
							ttl
						);

						// no need to wait
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-table-cached %s(%s) %d %d %s",
					prefixedTableName,
					hashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-table-cached %s(%s) %s %s",
					prefixedTableName,
					hashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryIndex(tableName, indexName, indexHashName, indexHash, desc) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(indexName);
		assertNonEmptyString(indexHashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				IndexName: indexName,
				KeyConditionExpression: "#hash = :hash",
				ExpressionAttributeNames: {
					"#hash": indexHashName
				},
				ExpressionAttributeValues: {
					":hash": indexHash
				},
				ScanIndexForward: desc === true ? false : true,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			return items;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-index %s %s %s %d %d %s",
					prefixedTableName,
					indexName,
					indexHashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-index %s %s %s %s %s",
					prefixedTableName,
					indexName,
					indexHashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryIndexRanged(tableName, indexName, indexHashName, indexHash, indexRangeName, indexRangeStart, indexRangeEnd, desc) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(indexName);
		assertNonEmptyString(indexHashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let KeyConditionExpression;
		const ExpressionAttributeNames = {
			"#hash": indexHashName
		};

		const ExpressionAttributeValues = {
			":hash": indexHash
		};

		if (indexRangeStart === undefined) {

			if (indexRangeEnd === undefined) {
				throw new Error();
			}
			else {

				KeyConditionExpression = "#hash = :hash and #range <= :priceEnd";
				ExpressionAttributeNames["#range"] = indexRangeName;
				ExpressionAttributeValues[":rangeEnd"] = indexRangeEnd;
			}
		}
		else {

			if (indexRangeEnd === undefined) {

				KeyConditionExpression = "#hash = :hash and :rangeStart <= #range";
				ExpressionAttributeNames["#range"] = indexRangeName;
				ExpressionAttributeValues[":rangeStart"] = indexRangeStart;
			}
			else {

				KeyConditionExpression = "#hash = :hash and #range between :rangeStart and :rangeEnd";
				ExpressionAttributeNames["#range"] = indexRangeName;
				ExpressionAttributeValues[":rangeStart"] = indexRangeStart;
				ExpressionAttributeValues[":rangeEnd"] = indexRangeEnd;
			}
		}

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				IndexName: indexName,
				KeyConditionExpression,
				ExpressionAttributeNames,
				ExpressionAttributeValues,
				ScanIndexForward: desc === true ? false : true,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			return items;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-index-ranged %s %s %s %d %d %s",
					prefixedTableName,
					indexName,
					indexHashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-index-ranged %s %s %s %s %s",
					prefixedTableName,
					indexName,
					indexHashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryIndexCached(ttl, tableName, hashName, rangeName, indexName, indexHashName, indexHash, desc) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);
		assertNonEmptyString(indexName);
		assertNonEmptyString(indexHashName);

		const prefixedTableName = this.tableNamePrefix + tableName;
		const forward = desc === true ? false : true;

		// group-user-pairs ! userId-createdAt-index ! user-1
		const setKey = `${prefixedTableName}!${indexName}!${indexHash}`;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			let items = await this.getCachedItems(
				prefixedTableName,
				setKey
			);

			if (items === undefined) {
				// ok
			}
			else {
				length = items.length;
				return items;
			}

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				IndexName: indexName,
				KeyConditionExpression: "#hash = :hash",
				ExpressionAttributeNames: {
					"#hash": indexHashName
				},
				ExpressionAttributeValues: {
					":hash": indexHash
				},
				ScanIndexForward: forward,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			items = response.Items;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;


			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();

						const ids = [];
						for (let i = 0; i < length; i++) {

							const item = items[i];

							// group-1_user-1
							// group-1_user-1 ! 123
							let id;
							if (rangeName === undefined) {
								id = item[hashName];
							}
							else {
								id = `${item[hashName]}!${item[rangeName]}`;
							}

							// group-user-pairs ! group-1_user-1
							// group-user-pairs ! group-1_user-1 ! 123
							const key = `${prefixedTableName}!${id}`;

							const json = JSON.stringify(
								item
							);

							multi.set(
								key,
								json,
								"EX", ttl
							);

							ids.push(i);
							ids.push(id);
						}

						// delete set
						multi.del(
							setKey
						);

						// add item ids to set
						multi.zadd(
							setKey,
							ids
						);

						// set ttl
						multi.expire(
							setKey,
							ttl
						);

						// no need to wait
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
					}
					catch (error) {
						this.log.warn(error)
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-index-cached %s %s %s %d %d %s",
					prefixedTableName,
					indexName,
					indexHashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-index-cached %s %s %s %s %s",
					prefixedTableName,
					indexName,
					indexHashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryIndexCachedVersioned(ttl, tableName, hashName, rangeName, indexName, indexHashName, indexHash, versionName, desc) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertOptionalNonEmptyString(rangeName);
		assertNonEmptyString(indexName);
		assertNonEmptyString(indexHashName);
		assertNonEmptyString(versionName);

		const prefixedTableName = this.tableNamePrefix + tableName;
		const forward = desc === true ? false : true;

		// group-user-pairs ! userId-createdAt-index ! user-1
		const setKey = `${prefixedTableName}!${indexName}!${indexHash}`;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			if (this.redis.connected) {

				try {

					const ids = await this.redis.zrangeAsync(setKey, 0, -1);

					length = ids.length;

					if (0 < length) {

						const multi = this.redis.multi();

						for (let i = 0; i < length; i++) {

							// group-1_user-1
							// group-1_user-1 ! 123
							const id = ids[i];

							// group-user-pairs ! group-1_user-1
							// group-user-pairs ! group-1_user-1 ! 123
							const key = `${prefixedTableName}!${id}`;

							//multi.get(key);

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

								if (item[versionName] === score) {
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
					)
				}
			}

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				IndexName: indexName,
				KeyConditionExpression: "#hash = :hash",
				ExpressionAttributeNames: {
					"#hash": indexHashName
				},
				ExpressionAttributeValues: {
					":hash": indexHash
				},
				ScanIndexForward: forward,
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

							const version = item[versionName];

							if (!Number.isFinite(version)) {
								throw new Error();
							}

							// group-1_user-1
							// group-1_user-1 ! 123
							let id;
							if (rangeName === undefined) {
								id = item[hashName];
							}
							else {
								id = `${item[hashName]}!${item[rangeName]}`;
							}

							// group-user-pairs ! group-1_user-1
							// group-user-pairs ! group-1_user-1 ! 123
							const key = `${prefixedTableName}!${id}`;

							const json = JSON.stringify(
								item
							);

							// versioned items are cached with 'zadd', not 'set'

							// multi.set(
							// 	key,
							// 	json,
							// 	"EX", ttl
							// );

							multi.zadd(
								key,
								version,
								json
							);

							multi.expire(
								key,
								ttl
							);

							ids.push(i, id);
						}

						// delete set
						multi.del(
							setKey
						);

						// add item ids to set
						multi.zadd(
							setKey,
							ids
						);

						// set ttl
						multi.expire(
							setKey,
							ttl
						);

						// no need to wait
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
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

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-index-cached-versioned %s %s %s %d %d %s",
					prefixedTableName,
					indexName,
					indexHashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-index-cached-versioned %s %s %s %s %s",
					prefixedTableName,
					indexName,
					indexHashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async queryIndexFirst(tableName, indexName, indexHashName, indexHash, desc) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(indexName);
		assertNonEmptyString(indexHashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				IndexName: indexName,
				Limit: 1,
				KeyConditionExpression: "#hash = :hash",
				ExpressionAttributeNames: {
					"#hash": indexHashName
				},
				ExpressionAttributeValues: {
					":hash": indexHash
				},
				ScanIndexForward: desc === true ? false : true,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			return items;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"query-index-first %s %s(%s) %d %d %s",
					prefixedTableName,
					indexName,
					indexHashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"query-index-first %s %s(%s) %s %s",
					prefixedTableName,
					indexName,
					indexHashName,
					caught.code,
					elapsed
				);
			}
		}
	}

	// async enumerateTable(tableName) {

	// 	assertNonEmptyString(tableName);

	// 	const prefixedTableName = this.tableNamePrefix + tableName;

	// 	let ExclusiveStartKey = null;
	// 	do {

	// 		const response = await this.ddb.scan({
	// 			TableName: prefixedTableName,
	// 			ExclusiveStartKey,
	// 			Limit: limit,
	// 			ReturnConsumedCapacity: "TOTAL"
	// 		}).promise();

	// 		const items = response.Items;
	// 		const lastEvaluatedKey = response.LastEvaluatedKey;

	// 		if (await iterator(items) === true) {
	// 			break;
	// 		}

	// 		ExclusiveStartKey = lastEvaluatedKey;

	// 	} while (ExclusiveStartKey);
	// }

	async enumerateTable(tableName, limit, exclusiveStartKey) {

		assertNonEmptyString(tableName);
		assertPositiveInteger(limit);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.scan({
				TableName: prefixedTableName,
				Limit: limit,
				ExclusiveStartKey: exclusiveStartKey,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;
			const lastEvaluatedKey = response.LastEvaluatedKey;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (lastEvaluatedKey === undefined) {

				return {
					items
				};
			}

			return {
				items,
				lastEvaluatedKey
			}
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"enumerate-table %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"enumerate-table %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async enumerateIndexRanged(tableName, indexName, indexHashName, indexHash, indexRangeName, indexRangeStart, indexRangeEnd, limit, exclusiveStartKey, desc) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(indexName);
		assertNonEmptyString(indexHashName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let KeyConditionExpression;
		const ExpressionAttributeNames = {
			"#hash": indexHashName
		};

		const ExpressionAttributeValues = {
			":hash": indexHash
		};

		if (indexRangeStart === undefined) {

			if (indexRangeEnd === undefined) {
				throw new Error();
			}
			else {

				KeyConditionExpression = "#hash = :hash and #range <= :priceEnd";
				ExpressionAttributeNames["#range"] = indexRangeName;
				ExpressionAttributeValues[":rangeEnd"] = indexRangeEnd;
			}
		}
		else {

			if (indexRangeEnd === undefined) {

				KeyConditionExpression = "#hash = :hash and :rangeStart <= #range";
				ExpressionAttributeNames["#range"] = indexRangeName;
				ExpressionAttributeValues[":rangeStart"] = indexRangeStart;
			}
			else {

				KeyConditionExpression = "#hash = :hash and #range between :rangeStart and :rangeEnd";
				ExpressionAttributeNames["#range"] = indexRangeName;
				ExpressionAttributeValues[":rangeStart"] = indexRangeStart;
				ExpressionAttributeValues[":rangeEnd"] = indexRangeEnd;
			}
		}

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const response = await this.ddb.query({
				TableName: prefixedTableName,
				IndexName: indexName,
				KeyConditionExpression,
				ExpressionAttributeNames,
				ExpressionAttributeValues,
				Limit: limit,
				ExclusiveStartKey: exclusiveStartKey,
				ScanIndexForward: desc === true ? false : true,
				ReturnConsumedCapacity: "TOTAL"
			}).promise();

			const items = response.Items;
			const lastEvaluatedKey = response.LastEvaluatedKey;

			length = items.length;
			consumed = response.ConsumedCapacity.CapacityUnits;

			if (lastEvaluatedKey === undefined) {

				return {
					items
				};
			}

			return {
				items,
				lastEvaluatedKey
			};
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"enumerate-index-ranged %s %s %s %d %d %s",
					prefixedTableName,
					indexName,
					indexHashName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"enumerate-index-ranged %s %s %s %s %s",
					prefixedTableName,
					indexName,
					indexHashName,
					caught.code,
					elapsed
				);
			}
		}
	}


	async batchGet(tableName, hash, range, hashes) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hash);
		assertOptionalNonEmptyString(range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length;
		let consumed = 0;

		const time = hrtime();

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

					map.set(
						hashValue,
						null
					);
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
				const items = response.Responses[
					prefixedTableName
				];

				if (0 < items.length) {

					this.log.debug(
						"got %d item(s).",
						items.length
					);

					for (const item of items) {

						const hashValue = item[hash];
						const value = map.get(
							hashValue
						);

						if (value === undefined) {

							this.log.error(
								"value not found."
							);

							throw new Error();
						}

						if (value === null) {

							map.set(
								hashValue,
								item
							);
						}
						else {

							this.log.error(
								"value already in map."
							);

							throw new Error();
						}

						results.push(
							item
						);
					}
				}

				const unprocessedKeys = response.UnprocessedKeys[
					prefixedTableName
				];

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

						const hashValue = key[
							hash
						];

						const value = map.get(
							hashValue
						);

						// check (optional)
						if (value === undefined) {

							this.log.error(
								"value not found."
							);

							throw new Error();
						}

						if (value === null) {
							// ok
						}
						else {

							this.log.error(
								"value already in map."
							);

							throw new Error();
						}

						queue.push(
							hash
						);
					}
				}
			} while (0 < queue.length);

			length = results.length;
			return results;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			this.log.trace(
				"batch-get %s %d %d %s",
				prefixedTableName,
				length,
				consumed,
				elapsed
			);
		}
	}

	async batchGetCached(ttl, tableName, hashName, hashes) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);

		if (Array.isArray(hashes)) {
			// ok
		}
		else {
			throw new Error();
		}

		if (0 < hashes.length) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const queue = [];
			const map = new Map();

			for (const hash of hashes) {

				if (map.has(hash)) {

					this.log.warn(
						"%j is duplicate.",
						hash
					);
				}
				else {

					queue.push(
						hash
					);

					map.set(
						hash,
						null
					);
				}
			}

			if (this.redis.connected) {

				try {

					const multi = this.redis.multi();
					for (const hash of queue) {

						const key = `${prefixedTableName}!${hash}`;
						multi.get(
							key
						);
					}

					const jsons = await multi.execAsync();

					if (jsons.indexOf(null) < 0) {

						const results = jsons.map(JSON.parse);
						length = results.length;
						return results;
					}
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			const results = [];

			do {

				// dequeue 100 ids from queue
				const chunk = queue.splice(0, 100);

				// prepare keys
				const keys = [];
				for (const hash of chunk) {

					keys.push({
						[hashName]: hash
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

					this.log.debug(
						"got %d item(s).",
						items.length
					);

					for (const item of items) {

						const hash = item[
							hashName
						];

						const value = map.get(
							hash
						);

						if (value === undefined) {

							this.log.error(
								"value not found."
							);

							throw new Error();
						}

						if (value === null) {

							map.set(
								hash,
								item
							);
						}
						else {

							this.log.error(
								"value already in map."
							);

							throw new Error();
						}

						results.push(
							item
						);
					}
				}

				const unprocessedKeys = response.UnprocessedKeys[
					prefixedTableName
				];

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

						const hash = key[
							hashName
						];

						const value = map.get(
							hash
						);

						// check (optional)
						if (value === undefined) {

							this.log.error(
								"value not found."
							);

							throw new Error();
						}

						if (value === null) {
							// ok
						}
						else {

							this.log.error(
								"value already in map."
							);

							throw new Error();
						}

						queue.push(
							hash
						);
					}
				}
			} while (0 < queue.length);

			length = results.length;

			// cache items
			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();

						for (const item of results) {

							const hash = item[
								hashName
							];

							const key = `${prefixedTableName}!${hash}`;

							const json = JSON.stringify(
								item
							);

							multi.set(
								key,
								json,
								"EX",
								ttl
							);
						}

						// no need to wait
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
					}
					catch (error) {

						this.log.warn(
							error
						);
					}
				}
			}

			return results;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"batch-get-cached %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"batch-get-cached %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}

	async batchGetCachedVersioned(ttl, tableName, hashName, versionName, hashes) {

		assertNonEmptyString(tableName);
		assertNonEmptyString(hashName);
		assertNonEmptyString(versionName);

		if (Array.isArray(hashes)) {
			// ok
		}
		else {
			throw new Error();
		}

		if (0 < hashes.length) {
			// ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		let length = 0;
		let consumed = 0;
		let caught;

		const time = hrtime();

		try {

			const queue = [];
			const map = new Map();

			for (const hash of hashes) {

				if (map.has(hash)) {

					this.log.warn(
						"%j is duplicate.",
						hash
					);
				}
				else {

					queue.push(
						hash
					);

					map.set(
						hash,
						null
					);
				}
			}

			if (this.redis.connected) {

				try {

					length = queue.length;

					const multi = this.redis.multi();

					for (const hash of queue) {

						const key = `${prefixedTableName}!${hash}`;

						multi.zrange(
							key,
							-1,
							-1,
							"WITHSCORES"
						);
					}

					const arrays = await multi.execAsync();

					let miss;
					const results = [];

					for (let i = 0; i < length; i++) {

						const array = arrays[i];
						if (Array.isArray(array)) {
							// ok
						}
						else {
							miss = true;
							break;
						}

						if (array.length === 2) {
							// ok
						}
						else {
							miss = true;
							break;
						}

						const json = array[0];

						if (typeof json === "string") {
							// ok
						}
						else {
							miss = true;
							break;
						}

						const scoreString = array[1];

						if (typeof scoreString === "string") {
							// ok
						}
						else {
							miss = true;
							break;
						}

						const item = JSON.parse(
							json
						);

						const score = Number.parseFloat(
							scoreString
						);

						if (Number.isFinite(score)) {
							// ok
						}
						else {
							miss = true;
							break;
						}

						const version = item[
							versionName
						];

						if (Number.isFinite(version)) {
							// ok
						}
						else {
							miss = true;
							break;
						}

						if (version === score) {
							// ok
						}
						else {

							miss = true;
							break;
						}

						results.push(
							item
						);
					}

					if (miss === true) {
						// ok
					}
					else {
						return results;
					}
				}
				catch (error) {

					this.log.warn(
						error
					);
				}
			}

			const results = [];

			do {

				// dequeue 100 ids from queue
				const chunk = queue.splice(0, 100);

				// prepare keys
				const keys = [];
				for (const hash of chunk) {

					keys.push({
						[hashName]: hash
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

					this.log.debug(
						"got %d item(s).",
						items.length
					);

					for (const item of items) {

						const hash = item[
							hashName
						];

						const value = map.get(
							hash
						);

						if (value === undefined) {

							this.log.error(
								"value not found."
							);

							throw new Error();
						}

						if (value === null) {

							map.set(
								hash,
								item
							);
						}
						else {

							this.log.error(
								"value already in map."
							);

							throw new Error();
						}

						results.push(
							item
						);
					}
				}

				const unprocessedKeys = response.UnprocessedKeys[
					prefixedTableName
				];

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

						const hash = key[
							hashName
						];

						const value = map.get(
							hash
						);

						// check (optional)
						if (value === undefined) {

							this.log.error(
								"value not found."
							);

							throw new Error();
						}

						if (value === null) {
							// ok
						}
						else {

							this.log.error(
								"value already in map."
							);

							throw new Error();
						}

						queue.push(
							hash
						);
					}
				}
			} while (0 < queue.length);

			length = results.length;

			// cache items
			if (0 < length) {

				if (this.redis.connected) {

					try {

						const multi = this.redis.multi();
						const ids = [];

						for (const item of results) {

							const hash = item[
								hashName
							];

							const key = `${prefixedTableName}!${hash}`;

							const version = item[
								versionName
							];

							if (Number.isFinite(version)) {
								// ok
							}
							else {
								throw new Error();
							}

							const json = JSON.stringify(
								item
							);

							multi.zadd(
								key,
								version,
								json
							);

							multi.expire(
								key,
								ttl
							);
						}

						// no need to wait
						multi.exec((error, reply) => {

							if (error) {
								this.log.warn(
									error
								);
							}
						});
					}
					catch (error) {

						this.log.warn(
							error
						)
					}
				}
			}

			return results;
		}
		catch (error) {

			caught = error;
			throw error;
		}
		finally {

			const [s, ns] = hrtime(time);
			const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

			if (caught === undefined) {

				this.log.trace(
					"batch-get-cached-versioned %s %d %d %s",
					prefixedTableName,
					length,
					consumed,
					elapsed
				);
			}
			else {

				this.log.warn(
					"batch-get-cached-versioned %s %s %s",
					prefixedTableName,
					caught.code,
					elapsed
				);
			}
		}
	}
}

DataAccess.prototype.log = null;
DataAccess.prototype.ddb = null;
DataAccess.prototype.redis = null;
DataAccess.prototype.tableNamePrefix = null;

module.exports = {
	DataAccess
};
