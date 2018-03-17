function createRepository(request) {

	const {
		prefix,
		tables: tableDefinitions
	} = request;

	if (prefix === undefined) {
		// ok
	}
	else if (typeof prefix === "string") {
		// ok
	}
	else {
		throw new Error();
	}

	class Repository { };
	const {
		prototype
	} = Repository;

	prototype.da = null;

	for (const rawTableName in tableDefinitions) {

		const tableDefinition = tableDefinitions[
			rawTableName
		];

		const tableName = prefix === undefined ? rawTableName : `${prefix}${rawTableName}`;

		const {
			hash: hashName,
			range: rangeName,
			version: versionName,
			ttl,
			methods: methodDefinitions
		} = tableDefinition;

		if (hashName === undefined) {
			throw new Error();
		}

		if (typeof hashName === "string") {
			if (0 < hashName.length) {
				// ok
			}
			else {
				throw new Error();
			}
		}
		else {
			throw new Error();
		}

		if (versionName === undefined) {
			// ok
		}
		else if (typeof versionName === "string") {
			if (0 < versionName.length) {
				// ok
			}
			else {
				throw new Error();
			}
		}
		else {
			throw new Error();
		}

		if (ttl === undefined) {
			// ok
		}
		else if (Number.isInteger(ttl)) {

			if (ttl < 0) {
				throw new Error();
			}
		}
		else {
			throw new Error();
		}

		for (const methodName in methodDefinitions) {

			if (prototype[methodName] === undefined) {
				// ok
			}
			else {
				throw new Error();
			}

			const methodDefinition = methodDefinitions[
				methodName
			];

			if (typeof methodDefinition === "string") {
				// ok
			}
			else {
				throw new Error();
			}

			const parts = methodDefinition.match(/[^ \t\r\n]+/g);
			if (parts === null) {
				throw new Error();
			}

			const type = parts[0];

			switch (type) {

				case "create":

					prototype[methodName] = function (item) {

						return this.da.create(
							tableName,
							hashName,
							item
						);
					};

					break;

				case "create-versioned":

					prototype[methodName] = function (item) {

						return this.da.createVersioned(
							tableName,
							hashName,
							versionName,
							item
						);
					};

					break;

				case "create-cached-versioned":

					if (versionName === undefined) {
						throw new Error();
					}

					if (ttl === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (item) {

						return this.da.createCachedVersioned(
							ttl,
							tableName,
							hashName,
							rangeName,
							versionName,
							item
						);
					};

					break;

				case "get":

					if (rangeName === undefined) {

						prototype[methodName] = function (hash) {

							return this.da.get(
								tableName,
								hashName,
								hash
							);
						};

					}
					else {

						prototype[methodName] = function (hash, range) {

							return this.da.get(
								tableName,
								hashName,
								hash,
								rangeName,
								range
							);
						};
					}

					break;

				case "get-consistent":

					prototype[methodName] = function (hash, range) {

						return this.da.getConsistent(
							tableName,
							hashName,
							hash,
							rangeName,
							range
						);
					};

					break;

				case "remove":

					prototype[methodName] = function (hash, range) {

						return this.da.remove(
							tableName,
							hashName,
							hash,
							rangeName,
							range
						);
					}
					break;

				case "remove-cached-versioned":

					if (versionName === undefined) {
						throw new Error();
					}

					if (rangeName === undefined) {

						prototype[methodName] = function (item) {

							return this.da.removeCachedVersioned(
								tableName,
								hashName,
								undefined,
								versionName,
								item
							);
						};
					}
					else {

						prototype[methodName] = function (item) {

							return this.da.removeCachedVersioned(
								tableName,
								hashName,
								rangeName,
								versionName,
								item
							);
						};
					}

					break;

				case "scan":
					prototype[methodName] = function () {

						return this.da.scan(
							tableName
						);
					}
					break;

				case "create-cached-versioned":

					if (versionName === undefined) {
						throw new Error();
					}

					if (ttl === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (item) {

						return this.da.createCachedVersioned(
							ttl,
							tableName,
							hashName,
							rangeName,
							versionName,
							item
						);
					}
					break;

				case "get-cached-versioned":

					if (versionName === undefined) {
						throw new Error();
					}

					if (ttl === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (hash) {

						return this.da.getCachedVersioned(
							ttl,
							tableName,
							hashName,
							hash,
							versionName
						);
					}

					break;

				case "query-index": {

					let indexName;
					switch (parts.length) {

						case 2:
							indexName = parts[1];
							break;

						case 3:
							indexName = parts[1];
							break;

						default:
							throw new Error();
					}

					const indexDefinition = tableDefinition.indices[
						indexName
					];

					if (indexDefinition === undefined) {
						throw new Error();
					}

					const {
						hash: indexHashName,
						range: indexRangeName
					} = indexDefinition;

					if (indexHashName === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (indexHash) {

						return this.da.queryIndex(
							tableName,
							indexName,
							indexHashName,
							indexHash
						);
					}

					break;
				}

				case "query-index-cached-versioned": {

					if (versionName === undefined) {
						throw new Error();
					}

					if (ttl === undefined) {
						throw new Error();
					}

					let indexName;
					switch (parts.length) {

						case 2:
							indexName = parts[1];
							break;

						case 3:
							indexName = parts[1];
							break;

						default:
							throw new Error();
					}

					const indexDefinition = tableDefinition.indices[
						indexName
					];

					if (indexDefinition === undefined) {
						throw new Error();
					}

					const {
						hash: indexHashName,
						range: indexRangeName
					} = indexDefinition;

					if (indexHashName === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (indexHash) {

						return this.da.queryIndexCachedVersioned(
							ttl,
							tableName,
							hashName,
							rangeName,
							indexName,
							indexHashName,
							indexHash,
							versionName
						);
					}

					break;
				}

				case "delete-cached":

					if (rangeName === undefined) {

						prototype[methodName] = function (hash) {

							return this.da.deleteCached(
								tableName,
								hashName,
								hash
							);
						}
					}
					else {

						prototype[methodName] = function (hash, range) {

							return this.da.deleteCached(
								tableName,
								hashName,
								hash,
								rangeName,
								range
							);
						}
					}

					break;

				case "update-versioned":

					if (versionName === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (item) {

						return this.da.updateVersioned(
							tableName,
							versionName,
							item
						);
					}
					break;

				case "update-cached-versioned":

					if (versionName === undefined) {
						throw new Error();
					}

					if (ttl === undefined) {
						throw new Error();
					}

					prototype[methodName] = function (item) {

						return this.da.updateCachedVersioned(
							ttl,
							tableName,
							hashName,
							rangeName,
							versionName,
							item
						);
					}
					break;

				default:
					throw new Error(type);
			}
		}

	}

	return Repository;
}

module.exports = {
	createRepository
};
