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

				}
			}
		}

		return Repository;
	}

	generateGetHash(prefixedTableName, hashName) {

		return async function (hash) {

			const time = process.hrtime();
			let item;
			let caught;

			try {
				const response = await this.ddb.get({
					TableName: prefixedTableName,
					Key: {
						[hashName]: hash
					}
				}).promise();

				item = response.Item;
				return item;
			}
			catch (error) {
				caught = error;
			}
			finally {

				const [s, ns] = process.hrtime(time);
				const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

				if (caught === undefined) {
					console.log("get %j(%j=%j) %d %s", prefixedTableName, hashName, hash, item === undefined ? 0 : 1, elapsed);
				}
				else {
					console.log("get %j(%j=%j) %j %s", prefixedTableName, hashName, hash, caught.code, elapsed);
				}
			}
		}
	}

	generateGetHashRange(prefixedTableName, hashName, rangeName) {

		return async function (hash, range) {

			const time = process.hrtime();
			let item;
			let caught;

			try {
				const response = await this.ddb.get({
					TableName: prefixedTableName,
					Key: {
						[hashName]: hash,
						[rangeName]: range
					}
				}).promise();

				item = response.Item;
				return item;
			}
			catch (error) {
				caught = error;
			}
			finally {

				const [s, ns] = process.hrtime(time);
				const elapsed = ((s * 1e9 + ns) / 1e6).toFixed(2);

				if (caught === undefined) {
					this.log.trace("get %j(%j=%j,%j=%j) %d %s", prefixedTableName, hashName, hash, rangeName, range, item === undefined ? 0 : 1, elapsed);
				}
				else {
					this.log.trace("get %j(%j=%j,%j=%j) %j %s", prefixedTableName, hashName, hash, rangeName, range, caught.code, elapsed);
				}
			}
		}
	}
}

RepositoryGenerator2.prototype.tableNamePrefix = null;

module.exports = {
	RepositoryGenerator2
};
