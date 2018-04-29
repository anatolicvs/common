"use strict";
const os = require("os");
const vm = require("vm");
const tools = require("./tools.js");

const kItemVersionKey = "--iv";

class RepositoryGenerator {

	generate(request) {

		const className = request.name;
		const methods = request.methods;
		const lines = [];
		let needsRedis = false;

		this.log.trace("generating repository class %j...", className);

		lines.push("(() => {");
		lines.push(`class ${className} {`);

		for (const name in methods) {

			const methodName = tools.getTrimmed(name, "methodName");

			let method = methods[name];
			if (typeof method === "string") {

				const parts = method.trim().split(/\s+/);
				if (parts.length === 2) {
					method = {
						type: parts[0],
						table: parts[1]
					};
				}
				else if (parts.length === 3) {

					method = {
						type: parts[0],
						table: parts[1],
						index: parts[2]
					};
				}
				else {
					throw new Error();
				}
			}

			const methodType = tools.getTrimmed(method.type, "method.type");

			switch (methodType) {

				case "create": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateCreate(lines, tableName, table, methodName, method);
					break;
				}

				case "update": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateUpdate(lines, tableName, table, methodName, method);
					break;
				}

				case "update-versioned": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateUpdateVersioned(lines, tableName, table, methodName, method);
					break;
				}

				case "put": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generatePut(lines, tableName, table, methodName, method);
					break;
				}

				case "delete": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateDelete(lines, tableName, table, methodName, method);
					break;
				}

				case "remove": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateRemove(lines, tableName, table, methodName, method);
					break;
				}

				case "get": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateGet(lines, tableName, table, methodName, method);
					break;
				}

				case "get-consistent": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateGetConsistent(lines, tableName, table, methodName, method);
					break;
				}

				case "get-cached": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					needsRedis = true;

					this.generateGetCached(lines, tableName, table, methodName, method);
					break;
				}

				case "scan-first": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.scanFirst(lines, tableName, table, methodName, method);
					break;
				}

				case "scan": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateScan(lines, tableName, table, methodName, method);
					break;
				}

				case "scan-cached": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					needsRedis = true;

					this.generateScanCached(lines, tableName, table, methodName, method);
					break;
				}

				case "query-table": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateQueryTable(lines, tableName, table, methodName, method);
					break;
				}

				case "query-table-cached": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					needsRedis = true;

					this.generateQueryTableCached(lines, tableName, table, methodName, method);
					break;
				}

				case "query-index": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const indexName = tools.getTrimmed(method.index, "method.index");
					const table = request.tables[tableName];
					const index = table.indices[indexName];

					this.generateQueryIndex(lines, tableName, table, indexName, index, methodName, method);
					break;
				}

				case "query-index-first": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const indexName = tools.getTrimmed(method.index, "method.index");
					const table = request.tables[tableName];
					const index = table.indices[indexName];

					this.generateQueryIndexFirst(lines, tableName, table, indexName, index, methodName, method);
					break;
				}

				case "query-index-cached": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const indexName = tools.getTrimmed(method.index, "method.index");
					const table = request.tables[tableName];
					const index = table.indices[indexName];

					needsRedis = true;

					this.generateQueryIndexCached(lines, tableName, table, indexName, index, methodName, method);
					break;
				}

				case "create-or-get": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateCreateOrGet(lines, tableName, table, methodName, method);
					break;
				}

				case "get-or-create": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateGetOrCreate(lines, tableName, table, methodName, method);
					break;
				}

				case "enumerate-table": {

					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateEnumerateTable(lines, tableName, table, methodName, method);
					break;
				}

				case "batch-get": {
					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateBatchGet(lines, tableName, table, methodName, method);
					break;
				}

				case "batch-get-cached": {
					const tableName = tools.getTrimmed(method.table, "method.table");
					const table = request.tables[tableName];

					this.generateBatchGetCached(lines, tableName, table, methodName, method);
					break;
				}

				default: {
					this.log.error(
						"methodType (%j) is unknown.",
						methodType
					);

					throw new Error();
				}
			}
		}

		lines.push("}");
		lines.push(`${className}.prototype.log = null;`);
		lines.push(`${className}.prototype.ddb = null;`);

		if (needsRedis) {
			lines.push(`${className}.prototype.redis = null;`);
		}

		lines.push(`return ${className};`);
		lines.push("})();");

		const code = lines.join(os.EOL);

		const value = vm.runInThisContext(code);
		return {
			name: className,
			code,
			value
		};
	}

	generateCreate(lines, tableName, table, methodName, method) {

		this.log.trace(
			"generating create %j...",
			tableName
		);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (item) {`);
		lines.push("");
		lines.push("	let consumed;");
		lines.push("	let caught;");
		lines.push("	const time = process.hrtime();");
		lines.push("");
		lines.push("	try {");
		lines.push("");
		lines.push("		const response = await this.ddb.put({");
		lines.push(`			TableName: "${prefixedTableName}",`);
		lines.push("			Item: item,");
		lines.push(`			ConditionExpression: "attribute_not_exists(${hash})",`);
		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");
		lines.push("");
		lines.push("		consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	}");
		lines.push("	catch(error) {");
		lines.push("");
		lines.push("		caught = error;");
		lines.push("		throw error;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push("		if (caught === undefined) {");
		lines.push(`			this.log.debug("'${methodName}' create ${prefixedTableName} %d %s", consumed, elapsed);`);
		lines.push("		}");
		lines.push("		else {");
		lines.push(`			this.log.debug("'${methodName}' create ${prefixedTableName} %j %s", caught.code, elapsed);`);
		lines.push("		}");
		lines.push("	}");
		lines.push("}");
	}

	generateCreateOrGet(lines, tableName, table, methodName, method) {

		this.log.trace("generating create-or-get %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (item) {`);
		lines.push("	let existingItem;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("");
		lines.push("	try {");
		lines.push("");
		lines.push("		let putResponse;");
		lines.push("		try {");
		lines.push("");
		lines.push("				putResponse = await this.ddb.put({");
		lines.push(`					TableName: "${prefixedTableName}",`);
		lines.push("					Item: item,");
		lines.push(`					ConditionExpression: "attribute_not_exists(${hash})",`);
		lines.push("					ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("				}).promise();");
		lines.push("				consumed = putResponse.ConsumedCapacity.CapacityUnits;");
		lines.push("				return;");
		lines.push("			}");
		lines.push("			catch (e) {");
		lines.push("");
		lines.push("				if (e.code !== \"ConditionalCheckFailedException\") {");
		lines.push("					throw e;");
		lines.push("				}");
		lines.push("			}");
		lines.push("");
		lines.push("		const getResponse = await this.ddb.get({");
		lines.push(`			TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`			Key: { ${hash}: item.${hash} },`);
		}
		else {
			lines.push(`			Key: { ${hash}: item.${hash}, ${range}: item.${range} },`);
		}

		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");
		lines.push("");
		lines.push("		consumed += getResponse.ConsumedCapacity.CapacityUnits;");
		lines.push("		existingItem = getResponse.Item;");
		lines.push("		return existingItem;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' create-or-get ${prefixedTableName} %d %d %s", existingItem === undefined ? 0 : 1, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateUpdate(lines, tableName, table, methodName, method) {

		this.log.trace("generating update %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (item) {`);
		lines.push("");
		lines.push("	const response = await this.ddb.put({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push("		Item: item,");
		lines.push(`		ConditionExpression: "attribute_exists(${hash})",`);
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("}");
	}

	generateUpdateVersioned(lines, tableName, table, methodName, method) {

		this.log.trace("generating update-versioned %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (item) {`);
		lines.push("");
		lines.push("	let consumed;");
		lines.push("	let caught;");
		lines.push("	const time = process.hrtime();");
		lines.push("");
		lines.push("	try {");
		lines.push("");
		lines.push(`		const iv = item["${kItemVersionKey}"];`);
		lines.push("");
		lines.push("		if (Number.isInteger(iv)) {");
		lines.push("");
		lines.push("			const nextiv = iv + 1;");
		lines.push("");
		lines.push("			const response = await this.ddb.put({");
		lines.push(`				TableName: "${prefixedTableName}",`);
		lines.push(`				Item: { ...item, ["${kItemVersionKey}"]: nextiv },`);
		lines.push(`				ConditionExpression: "#iv = :iv",`);
		lines.push("				ExpressionAttributeNames: {");
		lines.push(`					"#iv": "${kItemVersionKey}"`);
		lines.push("				},");
		lines.push("				ExpressionAttributeValues: {");
		lines.push(`					":iv": iv`);
		lines.push("				},");
		lines.push("				ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("			}).promise();");
		lines.push("");
		lines.push(`			item["${kItemVersionKey}"] = nextiv;`);
		lines.push("			consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("		}");
		lines.push("		else {");
		lines.push("");
		lines.push("			const response = await this.ddb.put({");
		lines.push(`			TableName: "${prefixedTableName}",`);
		lines.push(`				Item: { ...item, ["${kItemVersionKey}"]: 0 },`);
		lines.push(`				ConditionExpression: "attribute_exists(${hash}) and attribute_not_exists(#iv)",`);
		lines.push("				ExpressionAttributeNames: {");
		lines.push(`					"#iv": "${kItemVersionKey}"`);
		lines.push("				},");
		lines.push("				ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("			}).promise();");
		lines.push("");
		lines.push(`			item["${kItemVersionKey}"] = 0;`);
		lines.push("			consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("		}");
		lines.push("	}");
		lines.push("	catch(error) {");
		lines.push("");
		lines.push("		caught = error;");
		lines.push("		throw error;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push("		if (caught === undefined) {");
		lines.push(`			this.log.debug("'${methodName}' update-versioned ${prefixedTableName} %d %s", consumed, elapsed);`);
		lines.push("		}");
		lines.push("		else {");
		lines.push(`			this.log.debug("'${methodName}' update-versioned ${prefixedTableName} %j %s", caught.code, elapsed);`);
		lines.push("		}");
		lines.push("	}");
		lines.push("}");
	}

	generatePut(lines, tableName, table, methodName, method) {

		this.log.trace("generating put %j...", tableName);

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (item) {`);
		lines.push("	const response = await this.ddb.put({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push("		Item: item,");
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("}");
	}

	generateDelete(lines, tableName, table, methodName, method) {

		this.log.trace("generating delete %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		let range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		if (range === undefined) {
			lines.push(`async ${methodName} (hash) {`);
		}
		else {
			lines.push(`async ${methodName} (hash, range) {`);
		}

		lines.push("	const response = await this.ddb.delete({");
		lines.push(`		TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`		Key: { ${hash}: hash },`);
		}
		else {
			lines.push(`		Key: { ${hash}: hash, ${range}: range },`);
		}

		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("}");
	}

	generateRemove(lines, tableName, table, methodName, method) {

		this.log.trace("generating remove %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		let range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		if (range === undefined) {
			lines.push(`async ${methodName} (hash) {`);
		}
		else {
			lines.push(`async ${methodName} (hash, range) {`);
		}

		lines.push("	const response = await this.ddb.delete({");
		lines.push(`		TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`		Key: { ${hash}: hash },`);
		}
		else {
			lines.push(`		Key: { ${hash}: hash, ${range}: range },`);
		}

		lines.push(`		ConditionExpression: "attribute_exists(${hash})",`);
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("}");
	}

	generateGet(lines, tableName, table, methodName, method) {

		this.log.trace("generating get %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		if (range === undefined) {
			lines.push(`async ${methodName} (hash) {`);
		}
		else {
			lines.push(`async ${methodName} (hash, range) {`);
		}

		lines.push("	let item;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("		const response = await this.ddb.get({");
		lines.push(`			TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`			Key: { ${hash}: hash },`);
		}
		else {
			lines.push(`			Key: { ${hash}: hash, ${range}: range },`);
		}

		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");

		lines.push("		}).promise();");

		lines.push("		item = response.Item;");
		lines.push("		consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("		return item;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' get ${prefixedTableName} %d %d %s", item === undefined ? 0 : 1, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateGetConsistent(lines, tableName, table, methodName, method) {

		this.log.trace("generating get-consistent %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		if (range === undefined) {
			lines.push(`async ${methodName} (hash) {`);
		}
		else {
			lines.push(`async ${methodName} (hash, range) {`);
		}

		lines.push("	let item;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	const response = await this.ddb.get({");
		lines.push(`		TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`		Key: { ${hash}: hash },`);
		}
		else {
			lines.push(`		Key: { ${hash}: hash, ${range}: range },`);
		}

		lines.push("			ConsistentRead: true,");
		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");
		lines.push("		item = response.Item;");
		lines.push("		consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("		return item;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' get-consistent ${prefixedTableName} %d %d %s", item === undefined ? 0 : 1, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateGetCached(lines, tableName, table, methodName, method) {

		const ttl = 600;
		this.log.trace("generating get-cached %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		if (range === undefined) {
			lines.push(`async ${methodName} (hash) {`);
		}
		else {
			lines.push(`async ${methodName} (hash, range) {`);
		}

		lines.push("	let item;");

		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	if (this.redis.connected) {");
		lines.push("		try {");
		if (range === undefined) {
			lines.push(`			const json = await this.redis.getAsync(\`${prefixedTableName}!\${hash}\`);`);
		}
		else {
			lines.push(`			const json = await this.redis.getAsync(\`${prefixedTableName}!\${hash}!\${range}\`);`);
		}

		lines.push("			if (json !== null) {");
		lines.push("				item = JSON.parse(json);");
		lines.push("				return item;");
		lines.push("			}");
		lines.push("		}");
		lines.push("		catch (e) {");
		lines.push("			this.log.warn(e)");
		lines.push("		}");
		lines.push("	}");
		lines.push("	const response = await this.ddb.get({");
		lines.push(`		TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`		Key: { ${hash}: hash },`);
		}
		else {
			lines.push(`		Key: { ${hash}: hash, ${range}: range },`);
		}

		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");

		lines.push("	item = response.Item;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	if (item !== undefined) {");
		lines.push("		if (this.redis.connected) {");
		lines.push("			try {");

		if (range === undefined) {
			lines.push(`				const key = \`${prefixedTableName}!\${hash}\`;`);
		}
		else {
			lines.push(`				const key = \`${prefixedTableName}!\${hash}!\${range}\`;`);
		}

		lines.push(`				const json = JSON.stringify(item);`);
		lines.push(`				this.redis.multi().set(key, json).expire(key, ${ttl}).exec();`);
		lines.push("			}");
		lines.push("			catch (e) {");
		lines.push("				this.log.warn(e)");
		lines.push("			}");
		lines.push("		}");
		lines.push("	}");
		lines.push("	return item;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' get-cached ${prefixedTableName} %d %d %s", item === undefined ? 0 : 1, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	scanFirst(lines, tableName, table, methodName, method) {

		this.log.trace("generating scan-first...");

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName}() {`);
		lines.push("	const time = process.hrtime();");
		lines.push("	const response = await this.ddb.scan({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push(`		Limit: 1,`);
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const diff = process.hrtime(time);");
		lines.push("	const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`	this.log.debug("'${methodName}' scan-first ${prefixedTableName} %d %d %s", response.Items.length, response.ConsumedCapacity.CapacityUnits, elapsed);`);
		lines.push("	return response.Items;");
		lines.push("}");
	}

	generateScan(lines, tableName, table, methodName, method) {

		this.log.trace("generating scan...");

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName}() {`);
		lines.push("	const time = process.hrtime();");
		lines.push("	const response = await this.ddb.scan({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const diff = process.hrtime(time);");
		lines.push("	const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`	this.log.debug("'${methodName}' scan ${prefixedTableName} %d %d %s", response.Items.length, response.ConsumedCapacity.CapacityUnits, elapsed);`);
		lines.push("	return response.Items;");
		lines.push("}");
	}

	generateScanCached(lines, tableName, table, methodName, method) {

		this.log.trace("generating scan-cached...");

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);
		const ttl = 600;

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName}() {`);

		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	if (this.redis.connected) {");
		lines.push("		try {");
		lines.push(`			const ids = await this.redis.zrangeAsync("${prefixedTableName}", 0, -1);`);
		lines.push("			length = ids.length;");
		lines.push("			if (0 < length) {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				for (let i = 0; i < length; i++) {");
		lines.push("					const id = ids[i];");
		lines.push(`					const key = \`${prefixedTableName}!\${id}\`;`);
		lines.push("					multi.get(key);");
		lines.push("				}");
		lines.push("				const jsons = await multi.execAsync();");
		lines.push("				if (jsons.indexOf(null) < 0) {");
		lines.push("					return jsons.map(json => JSON.parse(json));");
		lines.push("				}");
		lines.push("			}");
		lines.push("		}");
		lines.push("		catch (e) {");
		lines.push("			this.log.warn(e)");
		lines.push("		}");
		lines.push("	}");
		lines.push("	const response = await this.ddb.scan({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const items = response.Items;");
		lines.push("	length = items.length;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	if (0 < length) {");
		lines.push("		if (this.redis.connected) {");
		lines.push("			try {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				const ids = [];");
		lines.push("				for (let i = 0; i < length; i++) {");
		lines.push("					const item = items[i];");

		// id is `${hash}` or `${hash}!${range}`
		if (range === undefined) {
			lines.push(`					const id = item.${hash};`);
		}
		else {
			lines.push(`					const id = \`\${item.${hash}}!\${item.${range}}\`;`);
		}

		lines.push(`					const key = \`${prefixedTableName}!\${id}\`;`);
		lines.push("					const json = JSON.stringify(item);");
		lines.push(`					multi.set(key, json, \"EX\", ${ttl});`);
		// lines.push(`					multi.expire(key, ${ttl});`);
		lines.push("					ids.push(i);");
		lines.push("					ids.push(id);");
		lines.push("				}");
		lines.push(`				multi.del("${prefixedTableName}");`);
		lines.push(`				multi.zadd("${prefixedTableName}", ids);`);
		lines.push(`				multi.expire("${prefixedTableName}", ${ttl});`);

		// no need to wait
		lines.push("				multi.exec();");
		lines.push("			}");
		lines.push("			catch (e) {");
		lines.push("				this.log.warn(e)");
		lines.push("			}");
		lines.push("		}");
		lines.push("	}");
		lines.push("	return items;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' scan-cached ${prefixedTableName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateQueryTable(lines, tableName, table, methodName, method) {

		this.log.trace("generating query-table...");

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hash) {`);
		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	const response = await this.ddb.query({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push(`		KeyConditionExpression: "${hash} = :hash",`);
		lines.push("		ExpressionAttributeValues: { \":hash\": hash },");
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const items = response.Items;");
		lines.push("	length = items.length;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	return items;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' query-table ${prefixedTableName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateQueryTableCached(lines, tableName, table, methodName, method) {

		this.log.trace("generating query-table...");

		const ttl = 600;

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hash) {`);
		lines.push(`	const setKey = \`${prefixedTableName}!\${hash}\`;`);

		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	if (this.redis.connected) {");
		lines.push("		try {");
		lines.push("			const ids = await this.redis.zrangeAsync(setKey, 0, -1);");
		lines.push("			length = ids.length;");
		lines.push("			if (0 < length) {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				for (const id of ids) {");
		lines.push(`					const key = \`${prefixedTableName}!\${id}\`;`);
		lines.push("					multi.get(key);");
		lines.push("				}");
		lines.push("				const jsons = await multi.execAsync();");
		lines.push("				if (jsons.indexOf(null) < 0) {");
		lines.push("					return jsons.map(json => JSON.parse(json));");
		lines.push("				}");
		lines.push("			}");
		lines.push("		}");
		lines.push("		catch (e) {");
		lines.push("			this.log.warn(e)");
		lines.push("		}");
		lines.push("	}");
		lines.push("	const response = await this.ddb.query({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push(`		KeyConditionExpression: "${hash} = :hash",`);
		lines.push("		ExpressionAttributeValues: { \":hash\": hash },");
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const items = response.Items;");
		lines.push("	length = items.length;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	if (0 < length) {");
		lines.push("		if (this.redis.connected) {");
		lines.push("			try {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				const ids = [];");
		lines.push("				for (let i = 0; i < length; i++) {");
		lines.push("					const item = items[i];");

		// id is `${hash}` or `${hash}!${range}`
		if (range === undefined) {
			lines.push(`					const id = item.${hash};`);
		}
		else {
			lines.push(`					const id = \`\${item.${hash}}!\${item.${range}}\`;`);
		}

		lines.push(`					const key = \`${prefixedTableName}!\${id}\`;`);
		lines.push("					const json = JSON.stringify(item);");
		lines.push(`					multi.set(key, json, \"EX\", ${ttl});`);
		lines.push("					ids.push(i);");
		lines.push("					ids.push(id);");
		lines.push("				}");
		lines.push("				multi.del(setKey);");
		lines.push("				multi.zadd(setKey, ids);");
		lines.push(`				multi.expire(setKey, ${ttl});`);

		// no need to wait
		lines.push("				multi.exec();");
		lines.push("			}");
		lines.push("			catch (e) {");
		lines.push("				this.log.warn(e)");
		lines.push("			}");
		lines.push("		}");
		lines.push("	}");
		lines.push("	return items;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' query-table-cached ${prefixedTableName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateQueryIndex(lines, tableName, table, indexName, index, methodName, method) {

		this.log.trace("generating query-index...");

		const hash = tools.getTrimmed(index.hash, "index.hash");

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hash) {`);
		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	const response = await this.ddb.query({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push(`		IndexName: "${indexName}",`);
		lines.push(`		KeyConditionExpression: "${hash} = :hash",`);
		lines.push("		ExpressionAttributeValues: { \":hash\": hash },");

		if (method.desc === true) {
			lines.push("		ScanIndexForward: false,");
		}

		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const items = response.Items;");
		lines.push("	length = items.length;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	return items;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' query-index ${prefixedTableName} ${indexName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateQueryIndexFirst(lines, tableName, table, indexName, index, methodName, method) {

		this.log.trace("generating query-index-first...");

		const hash = tools.getTrimmed(index.hash, "index.hash");

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hash) {`);
		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	const response = await this.ddb.query({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push(`		IndexName: "${indexName}",`);
		lines.push(`		Limit: 1,`);
		lines.push(`		KeyConditionExpression: "${hash} = :hash",`);
		lines.push("		ExpressionAttributeValues: { \":hash\": hash },");

		if (method.desc === true) {
			lines.push("		ScanIndexForward: false,");
		}

		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const items = response.Items;");
		lines.push("	length = items.length;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	return items;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' query-index-first ${prefixedTableName} ${indexName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateQueryIndexCached(lines, tableName, table, indexName, index, methodName, method) {

		this.log.trace("generating query-index-cached...");

		const ttl = 600;
		if (method.desc !== undefined) {
			throw new Error();
		}

		// get table hash
		let tableHash = tools.asTrimmed(table.hash);
		if (tableHash === undefined) {
			tableHash = "id";
		}

		// get table range
		const tableRange = tools.asTrimmed(table.range);

		// get index hash
		const indexHash = tools.getTrimmed(index.hash, "index.hash");

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hash) {`);
		lines.push(`	const setKey = \`${prefixedTableName}!${indexName}!\${hash}\`;`);
		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	if (this.redis.connected) {");
		lines.push("		try {");
		lines.push("			const ids = await this.redis.zrangeAsync(setKey, 0, -1);");
		lines.push("			length = ids.length;");
		lines.push("			if (0 < length) {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				for (let i = 0; i < length; i++) {");
		lines.push("					const id = ids[i];");
		lines.push(`					const key = \`${prefixedTableName}!\${id}\`;`);
		lines.push("					multi.get(key);");
		lines.push("				}");
		lines.push("				const jsons = await multi.execAsync();");
		lines.push("				if (jsons.indexOf(null) < 0) {");
		lines.push("					return jsons.map(json => JSON.parse(json));");
		lines.push("				}");
		lines.push("			}");
		lines.push("		}");
		lines.push("		catch (e) {");
		lines.push("			this.log.warn(e)");
		lines.push("		}");
		lines.push("	}");
		lines.push("	const response = await this.ddb.query({");
		lines.push(`		TableName: "${prefixedTableName}",`);
		lines.push(`		IndexName: "${indexName}",`);
		lines.push(`		KeyConditionExpression: "${indexHash} = :hash",`);
		lines.push("		ExpressionAttributeValues: { \":hash\": hash },");
		lines.push("		ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("	}).promise();");
		lines.push("	const items = response.Items;");
		lines.push("	length = items.length;");
		lines.push("	consumed = response.ConsumedCapacity.CapacityUnits;");
		lines.push("	if (0 < length) {");
		lines.push("		if (this.redis.connected) {");
		lines.push("			try {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				const ids = [];");
		lines.push("				for (let i = 0; i < length; i++) {");
		lines.push("					const item = items[i];");

		// id is `${hash}` or `${hash}!${range}`
		if (tableRange === undefined) {
			lines.push(`					const id = item.${tableHash};`);
		}
		else {
			lines.push(`					const id = \`\${item.${tableHash}}!\${item.${tableRange}}\`;`);
		}

		lines.push(`					const key = \`${prefixedTableName}!\${id}\`;`);
		lines.push("					const json = JSON.stringify(item);");
		lines.push(`					multi.set(key, json, \"EX\", ${ttl});`);
		// lines.push(`					multi.expire(key, ${ttl});`);
		lines.push("					ids.push(i);");
		lines.push("					ids.push(id);");
		lines.push("				}");
		lines.push("				multi.del(setKey);");
		lines.push("				multi.zadd(setKey, ids);");
		lines.push(`				multi.expire(setKey, ${ttl});`);

		// no need to wait
		lines.push("				multi.exec();");
		lines.push("			}");
		lines.push("			catch (e) {");
		lines.push("				this.log.warn(e)");
		lines.push("			}");
		lines.push("		}");
		lines.push("	}");
		lines.push("	return items;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' query-index-cached ${prefixedTableName} ${indexName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateGetOrCreate(lines, tableName, table, methodName, method) {

		this.log.trace("generating get-or-create %j...", tableName);

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (item) {`);
		lines.push("	let existingItem;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("		const getResponse = await this.ddb.get({");
		lines.push(`			TableName: "${prefixedTableName}",`);

		if (range === undefined) {
			lines.push(`		Key: { ${hash}: item.${hash} },`);
		}
		else {
			lines.push(`		Key: { ${hash}: item.${hash}, ${range}: item.${range} },`);
		}

		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");
		lines.push("		existingItem = getResponse.Item;");
		lines.push("		consumed = getResponse.ConsumedCapacity.CapacityUnits;");
		lines.push("		if (existingItem !== undefined) {");
		lines.push("			return existingItem;");
		lines.push("		}");
		lines.push("		const putResponse = await this.ddb.put({");
		lines.push(`			TableName: "${prefixedTableName}",`);
		lines.push("			Item: item,");
		lines.push(`			ConditionExpression: "attribute_not_exists(${hash})",`);
		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");
		lines.push("		consumed += putResponse.ConsumedCapacity.CapacityUnits;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' get-or-create ${prefixedTableName} %d %d %s", existingItem === undefined ? 0 : 1, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");

		// const id = item.id;

		// get(table, id, (err, existingItem) => {

		// 	if (err) {
		// 		return cb(err);
		// 	}

		// 	if (existingItem === undefined) {

		// 		create(table, item, err => {

		// 			if (err) {
		// 				return cb(err);
		// 			}

		// 			cb();
		// 		});
		// 	}
		// 	else {
		// 		cb(null, existingItem);
		// 	}
		// });
	}

	generateEnumerateTable(lines, tableName, table, methodName, method) {

		this.log.trace("generating enumerate-table...");

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (limit, iterator) {`);

		lines.push("	let ExclusiveStartKey = null;");
		lines.push("	do {");

		lines.push("		const response = await this.ddb.scan({");
		lines.push(`			TableName: "${prefixedTableName}",`);
		lines.push("			ExclusiveStartKey,");
		lines.push("			Limit: limit,");
		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");
		lines.push("		const items = response.Items;");
		lines.push("		const lastEvaluatedKey = response.LastEvaluatedKey;");
		lines.push("		if (await iterator(items) === true) {");
		lines.push("			break;");
		lines.push("		}");
		lines.push("		ExclusiveStartKey = lastEvaluatedKey;");
		lines.push("	} while (ExclusiveStartKey);");
		lines.push("}");
	}

	generateBatchGet(lines, tableName, table, methodName, method) {

		this.log.trace("generating batch-get...");

		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);
		if (range === undefined) {
			//ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hashes) {`);
		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	const queue = [];");
		lines.push("	const map = new Map();");
		lines.push("	const results = [];");
		lines.push("	for (const hash of hashes) {");
		lines.push("		if (map.has(hash)) {");
		lines.push("			this.log.warn(\"%j is duplicate.\", hash);");
		lines.push("		}");
		lines.push("		else {");
		lines.push("			queue.push(hash);");
		lines.push("			map.set(hash, null);");
		lines.push("		}");
		lines.push("	}");

		lines.push("	do {");

		// dequeue 100 ids from queue
		lines.push("		const chunk = queue.splice(0, 100);");

		// prepare keys
		lines.push("		const keys = [];");
		lines.push("		for (const hash of chunk) {");
		lines.push("			keys.push({");
		lines.push(`				"${hash}": hash`);
		lines.push("			});");
		lines.push("		}");
		lines.push('		this.log.debug("batch get %d id(s)...", chunk.length);');

		// batch get
		lines.push("		const response = await this.ddb.batchGet({");
		lines.push("			RequestItems: {");
		lines.push(`				"${prefixedTableName}": {`);
		lines.push("					Keys: keys");
		lines.push("				}");
		lines.push("			},");
		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");

		// accumulate consumed capacity units
		lines.push("	consumed += response.ConsumedCapacity[0].CapacityUnits;");

		// get items
		lines.push(`		const items = response.Responses["${prefixedTableName}"];`);
		lines.push("		if (0 < items.length) {");
		lines.push("			this.log.debug(\"got %d item(s).\", items.length);");
		lines.push("			for (const item of items) {");
		lines.push(`				const hash = item.${hash};`);
		lines.push("				const value = map.get(hash);");
		lines.push("				if (value === void 0) {");
		lines.push("					this.log.error(\"value not found.\");");
		lines.push("					throw new Error();");
		lines.push("				}");
		lines.push("				else if (value === null) {");
		lines.push("					map.set(hash, item);");
		lines.push("				}");
		lines.push("				else {");
		lines.push("					this.log.error(\"value already in map.\");");
		lines.push("					throw new Error();");
		lines.push("				}");

		lines.push("				results.push(item);");
		lines.push("			}");
		lines.push("		}");

		lines.push(`		const unprocessedKeys = response.UnprocessedKeys["${prefixedTableName}"];`);
		lines.push("		if (unprocessedKeys === void 0) {");
		lines.push("		}");
		lines.push("		else {");
		lines.push("			const keys = unprocessedKeys.Keys;");
		lines.push("			this.log.debug(\"%d id(s) are unprocessed.\", keys.length);");
		lines.push("			for (const key of keys) {");

		lines.push(`				const hash = key.${hash};`);
		lines.push("				const value = map.get(hash);");

		// check (optional)
		lines.push("				if (value === void 0) {");
		lines.push("					this.log.error(\"value not found.\");");
		lines.push("					throw new Error();");
		lines.push("				}");
		lines.push("				else if (value === null) {");
		lines.push("				}");
		lines.push("				else {");
		lines.push("					this.log.error(\"value already in map.\");");
		lines.push("					throw new Error();");
		lines.push("				}");
		lines.push("				queue.push(hash);");
		lines.push("			}");
		lines.push("		}");
		lines.push("	} while(0 < queue.length);");
		lines.push("	length = results.length;");
		lines.push("	return results;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' batch-get ${prefixedTableName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}

	generateBatchGetCached(lines, tableName, table, methodName, method) {

		this.log.trace("generating batch-get-cached...");

		const ttl = 600;
		let hash = tools.asTrimmed(table.hash);
		if (hash === undefined) {
			hash = "id";
		}

		const range = tools.asTrimmed(table.range);
		if (range === undefined) {
			//ok
		}
		else {
			throw new Error();
		}

		const prefixedTableName = this.tableNamePrefix + tableName;

		lines.push(`async ${methodName} (hashes) {`);

		lines.push("	let length;");
		lines.push("	let consumed = 0;");
		lines.push("	const time = process.hrtime();");
		lines.push("	try {");
		lines.push("	const queue = [];");
		lines.push("	const map = new Map();");
		lines.push("	let results;");

		lines.push("	for (const hash of hashes) {");
		lines.push("		if (map.has(hash)) {");
		lines.push("			this.log.warn(\"%j is duplicate.\", hash);");
		lines.push("		}");
		lines.push("		else {");
		lines.push("			queue.push(hash);");
		lines.push("			map.set(hash, null);");
		lines.push("		}");
		lines.push("	}");


		lines.push("	if (this.redis.connected) {");
		lines.push("		try {");

		lines.push("			const multi = this.redis.multi();");
		lines.push("			for (const hash of queue) {");
		lines.push(`				const key = \`${prefixedTableName}!\${hash}\`;`);
		lines.push("				multi.get(key);");
		lines.push("			}");
		lines.push("			const jsons = await multi.execAsync();");
		lines.push("			if (jsons.indexOf(null) < 0) {");
		lines.push("				results = jsons.map(json => JSON.parse(json));");
		lines.push("				length = results.length;");
		lines.push("				return results;");
		lines.push("			}");
		lines.push("		}");
		lines.push("		catch (e) {");
		lines.push("			this.log.warn(e)");
		lines.push("		}");
		lines.push("	}");

		lines.push("	results = [];");

		lines.push("	do {");

		// dequeue 100 ids from queue
		lines.push("		const chunk = queue.splice(0, 100);");

		// prepare keys
		lines.push("		const keys = [];");
		lines.push("		for (const hash of chunk) {");
		lines.push("			keys.push({");
		lines.push(`				"${hash}": hash`);
		lines.push("			});");
		lines.push("		}");
		lines.push('		this.log.debug("batch get %d id(s)...", chunk.length);');

		// batch get
		lines.push("		const response = await this.ddb.batchGet({");
		lines.push("			RequestItems: {");
		lines.push(`				"${prefixedTableName}": {`);
		lines.push("					Keys: keys");
		lines.push("				}");
		lines.push("			},");
		lines.push("			ReturnConsumedCapacity: \"TOTAL\"");
		lines.push("		}).promise();");

		// accumulate consumed capacity units
		lines.push("	consumed += response.ConsumedCapacity[0].CapacityUnits;");

		// get items
		lines.push(`		const items = response.Responses["${prefixedTableName}"];`);
		lines.push("		if (0 < items.length) {");
		lines.push("			this.log.debug(\"got %d item(s).\", items.length);");
		lines.push("			for (const item of items) {");
		lines.push(`				const hash = item.${hash};`);
		lines.push("				const value = map.get(hash);");
		lines.push("				if (value === void 0) {");
		lines.push("					this.log.error(\"value not found.\");");
		lines.push("					throw new Error();");
		lines.push("				}");
		lines.push("				else if (value === null) {");
		lines.push("					map.set(hash, item);");
		lines.push("				}");
		lines.push("				else {");
		lines.push("					this.log.error(\"value already in map.\");");
		lines.push("					throw new Error();");
		lines.push("				}");
		lines.push("				results.push(item);");
		lines.push("			}");
		lines.push("		}");
		lines.push(`		const unprocessedKeys = response.UnprocessedKeys["${prefixedTableName}"];`);
		lines.push("		if (unprocessedKeys === void 0) {");
		lines.push("		}");
		lines.push("		else {");
		lines.push("			const keys = unprocessedKeys.Keys;");
		lines.push("			this.log.debug(\"%d id(s) are unprocessed.\", keys.length);");
		lines.push("			for (const key of keys) {");
		lines.push(`				const hash = key.${hash};`);
		lines.push("				const value = map.get(hash);");

		// check (optional)
		lines.push("				if (value === void 0) {");
		lines.push("					this.log.error(\"value not found.\");");
		lines.push("					throw new Error();");
		lines.push("				}");
		lines.push("				else if (value === null) {");
		lines.push("				}");
		lines.push("				else {");
		lines.push("					this.log.error(\"value already in map.\");");
		lines.push("					throw new Error();");
		lines.push("				}");

		lines.push("				queue.push(hash);");
		lines.push("			}");
		lines.push("		}");
		lines.push("	} while(0 < queue.length);");
		lines.push("	length = results.length;");

		// cache items
		lines.push("	if (0 < length) {");
		lines.push("		if (this.redis.connected) {");
		lines.push("			try {");
		lines.push("				const multi = this.redis.multi();");
		lines.push("				for (const item of results) {");
		lines.push(`					const hash = item.${hash};`);
		lines.push(`					const key = \`${prefixedTableName}!\${hash}\`;`);
		lines.push("					const json = JSON.stringify(item);");
		lines.push(`					multi.set(key, json, \"EX\", ${ttl});`);
		lines.push("				}");

		// no need to wait
		lines.push("				multi.exec();");
		lines.push("			}");
		lines.push("			catch (e) {");
		lines.push("				this.log.warn(e)");
		lines.push("			}");
		lines.push("		}");
		lines.push("	}");
		lines.push("	return results;");
		lines.push("	}");
		lines.push("	finally {");
		lines.push("		const diff = process.hrtime(time);");
		lines.push("		const elapsed = ((diff[0] * 1e9 + diff[1]) / 1e6).toFixed(2);");
		lines.push(`		this.log.debug("'${methodName}' batch-get-cached ${prefixedTableName} %d %d %s", length, consumed, elapsed);`);
		lines.push("	}");
		lines.push("}");
	}
}

RepositoryGenerator.prototype.log = null;
RepositoryGenerator.prototype.generateLog = true;
RepositoryGenerator.prototype.tableNamePrefix = null;

module.exports = {
	RepositoryGenerator
};