"use strict";
const assert = require("assert");
const net = require("net");
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

	it("shoud get public ip", cb => {
		tools.getPublicIP((err, ip) => {

			if (err) {
				return cb(err);
			}

			assert(net.isIP(ip));
			cb();
		});
	})
});
