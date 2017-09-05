"use strict";
const assert = require("assert");
const net = require("net");
const { tools } = require("..");

describe('isString', () => {

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

	tests.forEach(test => {

		it("correctly tests " + test.args, () => {
			const result = tools.isString(test.args);
			assert.equal(result, test.expected);
		});
	});
});


describe('isEmail', () => {

	const tests = [
		{ expected: false },
		{ args: void 0, expected: false },
		{ args: undefined, expected: false },
		{ args: null, expected: false },
		{ args: 0, expected: false },
		{ args: 0.5, expected: false },
		{ args: {}, expected: false },
		{ args: [], expected: false },
		{ args: "a@b.com", expected: true },
	];

	tests.forEach(test => {

		it("correctly tests " + test.args, () => {
			const result = tools.isEmail(test.args);
			assert.equal(result, test.expected);
		});
	});
});

describe('isId', () => {

	const tests = [
		{ expected: false },
		{ args: void 0, expected: false },
		{ args: undefined, expected: false },
		{ args: null, expected: false },
		{ args: 0, expected: false },
		{ args: 0.5, expected: false },
		{ args: {}, expected: false },
		{ args: [], expected: false },
		{ args: "00000000-0000-0000-0000-000000000000", expected: true },
	];

	tests.forEach(test => {

		it("correctly tests " + test.args, () => {
			const result = tools.isId(test.args);
			assert.equal(result, test.expected);
		});
	});
});

describe('getPublicIP', () => {

	it("shoud get an ip", cb => {

		tools.getPublicIP((err, ip) => {

			if (err) {
				return cb(err);
			}

			assert(net.isIP(ip));
			cb();
		});
	})
});


describe('creditCardExp', () => {

	it("shoud work", () => {

		for (let i = 10000; i < 20000; i++) {

			const exp = i.toString().substr(1);
			if (tools.isCreditCardExp(exp)) {
				const month = exp.substr(0, 2);
				const year = exp.substr(2);

				assert(month.length === 2);
				const monthValue = parseInt(month);
				assert(1 <= monthValue);
				assert(monthValue <= 12);

				assert(year.length === 2);
			}
		}
	})
});
