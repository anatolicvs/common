"use strict";
const assert = require("assert");
const { tools } = require("..");

describe('isString', function () {

	const tests = [
		{ expected: false },
		{ args: void 0, expected: false },
		{ args: undefined, expected: false },
		{ args: null, expected: false },
		{ args: 0, expected: false },
		{ args: 0.5, expected: false },
		{ args: {}, expected: false },
		{ args: [], expected: false },
		{ args: '', expected: true },
		{ args: "", expected: true },
		{ args: ``, expected: true },
		{ args: 'payload', expected: true },
		{ args: "payload", expected: true },
		{ args: `payload`, expected: true },
	];

	tests.forEach(function (test) {

		it("correctly tests " + test.args, function () {
			const result = tools.isString(test.args);
			assert.equal(result, test.expected);
		});
	});
});
