"use strict";
const assert = require("assert");
const { tools } = require("..");

const negative = [
	undefined,
	null,
	true,
	false,
	-123,
	-0.5,
	-0,
	0,
	0.5,
	123,
	[],
	[{}, 123, "gogo"],
	"",
	"test",
	"123",
	function () { },
	async function () { },
	() => { },
	async () => { }
];

const positive = [
	{},
	{ a: 1 },
	{ a: 1, b: [] }
];

describe("tools", () => {

	describe('isObject', () => {

		negative.forEach(value => {

			it("correctly tests " + value, () => {
				const result = tools.isObject(value);
				assert.equal(result, false);
			});
		});

		positive.forEach(value => {

			it("correctly tests " + value, () => {
				const result = tools.isObject(value);
				assert.equal(result, true);
			});
		});
	});

});
