"use strict";
const { deepStrictEqual } = require("assert");
const { validate } = require("../src/validate");

const scenarios = [
	{ schema: "integer", instance: 1 },
	{ schema: "number", instance: 1.1 },
	{ schema: "string", response: ["$ is required."] },
	{ schema: "string", instance: null, response: ["$ (null) is not a string."] },
	{ schema: "string", instance: "" },
	{ schema: "string", instance: "1", },
	{ schema: "string", instance: "2", },
	{ schema: "string", instance: 3, response: ["$ (3) is not a string."] },
	{ schema: { type: "string" } },
	{ schema: { type: "string", required: true }, response: ["$ is required."] },
	{ schema: { type: "string", required: true }, instance: 3, response: ["$ (3) is not a string."] },

	{ schema: ["string", "integer"], response: [[["$ is required."], ["$ is required."]]] },
	{ schema: ["string", "integer"], instance: null, response: [[["$ (null) is not a string."], ["$ (null) is not an integer."]]] },
	{ schema: ["string", "integer"], instance: 123.5, response: [[["$ (123.5) is not a string."], ["$ (123.5) is not an integer."]]] },
	{ schema: ["string", "integer"], instance: "" },
	{ schema: ["string", "integer"], instance: "abc" },
	{ schema: ["string", "integer"], instance: 123 },
	{ schema: ["string", "integer"], instance: 123 },
	{ schema: ["string", "integer", "boolean"], instance: false },

	{
		schema: {
			type: "object",
			properties: {
				id: ["string", "integer"]
			}
		},
		instance: {
			id: "abc"
		}
	},

	{
		schema: {
			type: "object",
			properties: {
				id: ["string", "integer"]
			}
		},
		instance: {
			id: 123
		}
	},

	{
		schema: {
			type: "object",
			properties: {
				id: ["string", {
					type: "object",
					required: true,
					properties: {
						id: "string"
					}
				}]
			}
		},
		instance: {
			id: {
				id: "123"
			}
		}
	}
];

const root = [
	{
		type: "object",
		properties: {
			type: [
				{
					type: "constant", value: "object"
				},
				{
					type: "constant", value: "string"
				},
				{
					type: "constant", value: "number"
				},
				{
					type: "constant", value: "integer"
				}
			],
			required: {
				type: "boolean"
			}
		}
	},
	{
		type: "array"
	},
	{
		type: "constant", value: "string"
	},
	{
		type: "constant", value: "number"
	},
	{
		type: "constant", value: "integer"
	}
];

describe("validate", () => {

	it("should validate root by itself", () => {

		const errors = validate(root, root, "$");
		if (errors === undefined) {
			// ok
		}
		else {
			throw new Error();
		}
	});

	for (const scenario of scenarios) {

		it("should handle ..", () => {

			const errors = validate(root, scenario.schema, "$");
			if (errors === undefined) {
				// ok
			}
			else {
				throw new Error();
			}

			const response = validate(scenario.schema, scenario.instance, "$");

			deepStrictEqual(response, scenario.response);

		});
	}
});
