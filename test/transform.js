"use strict";
const assert = require("assert");
const {
	transform
} = require("../src/transform");

const tests = [

	{ expression: void 0, expected: void 0 },

	{ expression: true, expected: true },
	{ expression: false, expected: false },
	{ expression: 123, expected: 123 },
	{ expression: "hey", expected: "hey" },
	{ expression: null, expected: null },
	{ expression: [], expected: [] },
	{ expression: [true, false, 123, "hey", []], expected: [true, false, 123, "hey", []] },
	{ expression: {}, expected: {} },

	{ expression: { $undefined: undefined }, expected: undefined },
	{ expression: { $undefined: null }, throws: "" },
	{ expression: { $boolean: true }, expected: true },
	{ expression: { $boolean: false }, expected: false },
	{ expression: { $boolean: 123 }, throws: "" },

	{ expression: { $number: 123 }, expected: 123 },
	{ expression: { $number: false }, throws: "" },
	{ expression: { $string: "hey" }, expected: "hey" },
	{ expression: { $string: 123 }, throws: "" },
	{ expression: { $object: null }, expected: null },
	{ expression: { $object: {} }, expected: {} },
	{ expression: { $object: 123 }, throws: "" },
	{ expression: { $array: [] }, expected: [] },
	{ expression: { $array: "hey" }, throws: "" },

	{ expression: { $array: [{ $array: [] }] }, expected: [[]] },

	{
		expression: () => { }, throws: ""
	},

	{
		expression: {
			$object: {
				"name": "gogo"
			}
		}, expected: { name: "gogo" }
	},

	{
		expression: {
			$object: {
				"name": "gogo"
			}
		}, expected: { name: "gogo" }
	},

	{
		expression: {
			$object: [
				{ name: "name", value: "gogo" }
			]
		}, expected: { name: "gogo" }
	},

	{
		expression: {
			$object: {
				"name": "gogo"
			}
		},
		context: { "var1": 345 },
		expected: { name: "gogo" }
	},

	{
		expression: {
			$lookup: "var1"
		},
		context: { "var1": 345 },
		expected: 345
	},

	{
		expression: {
			$lookup: {
				$lookup: "var2"
			}
		},
		context: { "var1": 345, "var2": "var1" },
		expected: 345
	},

	{
		expression: {
			$object: {
				"test": {
					$lookup: "var1"
				}
			}
		},
		context: { "var1": 345 },
		expected: {
			test: 345
		}
	},

	{
		expression: {
			$object: {
				"test": {
					$lookup: "null"
				}
			}
		},
		context: { "var1": 345 },
		expected: {}
	},

	{
		expression: {
			$object: [
				{
					name: {
						$lookup: "name-1"
					},
					value: {
						$lookup: "null"
					}
				}
			]
		},
		context: {
			"name-1": "test"
		},
		expected: {}
	},

	{
		expression: {
			$object: [
				{
					name: {
						$lookup: "name-1"
					},
					value: {
						$lookup: "value-1"
					}
				}
			]
		},
		context: {
			"name-1": "test",
			"value-1": 345
		},
		expected: {
			test: 345
		}
	},

	{
		expression: {
			$add: {
				left: 1,
				right: {
					$add: {
						left: 2,
						right: 3
					}
				}
			}
		},
		expected: 6
	},

	{
		expression: {
			$concat: [
				"hello, ",
				"world!"
			]
		},
		expected: "hello, world!"
	},

	{
		expression: {
			$concat: {
				left: "hello, ",
				right: "world!"
			}
		},
		expected: "hello, world!"
	},

	{
		expression: {
			foo: {
				field: 1
			},
			bar: {
				field: 2
			}
		},
		expected: {
			foo: {
				field: 1
			},
			bar: {
				field: 2
			}
		}
	},

	{
		expression: {
			$object: [
				{
					name: "$object",
					value: {}
				}
			]
		},
		expected: {
			$object: {}
		}
	},
];

describe("transform", () => {

	for (const test of tests) {

		let name;
		switch (typeof test.expression) {
			case "undefined":
				name = "undefined";
				break;

			case "function":
				name = "function";
				break;

			default:
				name = JSON.stringify(test.expression);
		}

		it(name, () => {

			if (test.throws === undefined) {

				const actual = transform(
					test.expression,
					test.context
				);

				assert.deepStrictEqual(actual, test.expected);
			}
			else {

				try {

					transform(
						test.expression
					);
				}
				catch (error) {

					assert(error.message === test.throws);
					return;
				}

				throw new Error();
			}
		})
	}
});