"use strict";

class RepositoryBase {
}

RepositoryBase.prototype.log = null;
RepositoryBase.prototype.ddb = null;

class RepositoryGenerator2 {

	generate(tables) {

		class Repository extends RepositoryBase { }
		const prototype = Repository.prototype;

		for (const tableName in tables) {

			const tableInfo = tables[tableName];

			const prefixedTableName = `${this.tableNamePrefix}${tableName}`;

			for (const methodName in tableInfo.methods) {

				const methodInfo = tableInfo.methods[methodName];

				switch (methodInfo.type) {

					case "get": {

						if (tableInfo.range === undefined) {
							prototype[methodName] = this.generateGetHash(
								prefixedTableName,
								tableInfo.hash
							);
						}
						else {
							prototype[methodName] = this.generateGetHashRange(
								prefixedTableName,
								tableInfo.hash,
								tableInfo.range
							);
						}

						break;
					}

					case "create": {

						if (tableInfo.versioned === true) {

							prototype[methodName] = this.generateCreateVersioned(
								prefixedTableName,
								tableInfo.hash,
								tableInfo.versionProperty
							);
						}
						else {

							prototype[methodName] = this.generateCreate(
								prefixedTableName,
								tableInfo.hash
							);
						}

						break;
					}
				}
			}
		}

		return Repository;
	}

	generateGetHash(prefixedTableName, hashName) {

		return async function (hash) {

			const time = process.hrtime();
			let item;
			let consumed;
			let caught;

			try {

				const response = await this.ddb.get({
					TableName: prefixedTableName,
					Key: {
						[hashName]: hash
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				item = response.Item;
				consumed = response.ConsumedCapacity.CapacityUnits;

				return item;
			}
			catch (error) {
				caught = error;
				throw error;
			}
			finally {

				const [s, ns] = process.hrtime(time);
				const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

				if (caught === undefined) {
					this.log.trace("get %s(%s=%j) %d %d %s", prefixedTableName, hashName, hash, item === undefined ? 0 : 1, consumed, elapsed);
				}
				else {
					this.log.trace("get %s(%s=%j) %s %s", prefixedTableName, hashName, hash, caught.code, elapsed);
				}
			}
		}
	}

	generateGetHashRange(prefixedTableName, hashName, rangeName) {

		return async function (hash, range) {

			const time = process.hrtime();
			let item;
			let consumed;
			let caught;

			try {

				const response = await this.ddb.get({
					TableName: prefixedTableName,
					Key: {
						[hashName]: hash,
						[rangeName]: range
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				item = response.Item;
				consumed = response.ConsumedCapacity.CapacityUnits;

				return item;
			}
			catch (error) {

				caught = error;
				throw error;
			}
			finally {

				const [s, ns] = process.hrtime(time);
				const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

				if (caught === undefined) {
					this.log.trace("get %s(%s=%j,%s=%j) %d %d %s", prefixedTableName, hashName, hash, rangeName, range, item === undefined ? 0 : 1, consumed, elapsed);
				}
				else {
					this.log.trace("get %s(%s=%j,%s=%j) %s %s", prefixedTableName, hashName, hash, rangeName, range, caught.code, elapsed);
				}
			}
		}
	}

	generateCreate(prefixedTableName, hashName) {

		return async function (item) {

			const time = process.hrtime();
			let consumed;
			let caught;

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

				const [s, ns] = process.hrtime(time);
				const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

				if (caught === undefined) {
					this.log.debug("create %s %d %s", prefixedTableName, consumed, elapsed);
				}
				else {
					this.log.debug("create %s %s %s", prefixedTableName, caught.code, elapsed);
				}
			}
		}
	}

	generateCreateVersioned(prefixedTableName, hashName, versionProperty) {

		return async function (item) {

			const time = process.hrtime();
			let consumed;
			let caught;

			try {

				const response = await this.ddb.put({
					TableName: prefixedTableName,
					Item: { ...item, [versionProperty]: 0 },
					ConditionExpression: "attribute_not_exists(#hash)",
					ExpressionAttributeNames: {
						"#hash": hashName
					},
					ReturnConsumedCapacity: "TOTAL"
				}).promise();

				item[versionProperty] = 0;

				consumed = response.ConsumedCapacity.CapacityUnits;
			}
			catch (error) {

				caught = error;
				throw error;
			}
			finally {

				const [s, ns] = process.hrtime(time);
				const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

				if (caught === undefined) {
					this.log.debug("create-versioned %s %d %s", prefixedTableName, consumed, elapsed);
				}
				else {
					this.log.debug("create-versioned %s %s %s", prefixedTableName, caught.code, elapsed);
				}
			}
		}
	}
}

RepositoryGenerator2.prototype.tableNamePrefix = null;

module.exports = {
	RepositoryGenerator2
};
