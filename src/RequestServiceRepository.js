"use strict";
const {
	createRepository
} = require("./createRepository");

const RequestServiceRepository = createRepository({
	prefix: "request.",
	tables: {
		"requests": {
			hash: "id",
			version: "iv",
			indices: {
				"accountId-createdAt-index": {
					hash: "accountId",
					range: "createdAt"
				}
			},
			methods: {
				createRequest: "create-versioned",
				updateRequest: "update-versioned",
				getRequest: "get",
				queryRequestsByAccount: "query-index accountId-createdAt-index"
			}
		}
	}
});


module.exports = {
	RequestServiceRepository
};
