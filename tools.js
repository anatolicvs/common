"use strict";

const path = require("path");
const util = require("util");
const crypto = require("crypto");
const uuid = require("uuid");
const lodash = require("lodash");
const restify = require("restify");
const moment = require("moment");

/*******************************
	patterns
*******************************/
const patterns = {
	// http://stackoverflow.com/questions/46155/validate-email-address-in-javascript
	email: /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i,

	uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
	uuid_uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
	code: /^(?=.{1,1024}$)([0-9a-z]+(-[0-9a-z]+)*)$/,
	adminUsername: /^[a-zA-Z]{3,32}$/,

	managerUsername: /^[a-zA-Z]{3,32}@[a-zA-Z]{3,32}$/,

	// https://www.thepolyglotdeveloper.com/2015/05/use-regex-to-test-password-strength-in-javascript/
	strongPassword: /^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#\$%\^&\*])(?=.{8,})/,

	phone: /^[0-9]{10}$/,
	phoneVerify: /^[1-9][0-9]{5}$/,

	hex64: /^[0-9a-fA-F]{64}$/,

	creditCardNo: /^[0-9]{16}$/,
	creditCardCVC: /^[0-9]{3}$/,
	creditCardExp: /^(0?[1-9]|1[012])([0-9]{2})$/
};
module.exports.patterns = patterns;

/*******************************
	String.prototype.format
*******************************/
String.prototype.format = function (...args) {

	return util.format(this, ...args);
};


/*******************************
	Array.prototype.removeIf
*******************************/
Array.prototype.removeIf = function (cb) {

	let removed = 0;
	for (let i = this.length - 1; i >= 0; i--) {

		if (cb(this[i], i)) {
			this.splice(i, 1);
			removed++;
		}
	}
	return removed;
};

/*******************************
	exportAll
*******************************/
function exportAll(module, array) {

	for (let i = 0, length = array.length; i < length; i++) {

		const item = array[i];
		module.exports[item.name] = item;
	}
}


/*******************************
	format
*******************************/
module.exports.format = util.format;


/*******************************
	ts
*******************************/
function ts() {
	return Date.now();
}


/*******************************
	newid
*******************************/
function newid() {
	return uuid.v4();
}


/*******************************
	isArray
*******************************/
function isArray(value) {
	return lodash.isArray(value);
}


/*******************************
	isFinite
*******************************/
function isFinite(value) {
	return lodash.isFinite(value);
};


/*******************************
	isInteger
*******************************/
function isInteger(value) {
	return lodash.isInteger(value);
};


/*******************************
	isString
*******************************/
function isString(value) {
	return lodash.isString(value);
}


/*******************************
	isObject
*******************************/
function isObject(value) {
	return lodash.isPlainObject(value);
}


/*******************************
	isFunction
*******************************/
function isFunction(value) {
	return lodash.isFunction(value);
}


/*******************************
	isId
*******************************/
function isId(value) {
	return patterns.uuid.test(value);
}


/*******************************
	getId
*******************************/
function getId(value) {

	if (isString(value)) {

		value = value.trim().toLowerCase();

		if (isId(value)) {
			return value;
		}
	}
}


/*******************************
	getTrimmed
*******************************/
function getTrimmed(value) {

	if (isString(value)) {

		value = value.trim();

		if (value.length > 0) {
			return value;
		}
	}
}


/*******************************
	getEmail
*******************************/
function getEmail(value) {

	if (patterns.email.test(value)) {
		return value;
	}
}


/*******************************
	isEmail
*******************************/
function isEmail(value) {
	return patterns.email.test(value);
}


/*******************************
	module.exports.isCode
*******************************/
function isCode(value) {
	return patterns.code.test(value);
}


/*******************************
	isPhone
*******************************/
function isPhone(value) {
	return patterns.phone.test(value);
}


/*******************************
	round2dec
*******************************/
function round2dec(value) {
	if (!isFinite(value)) {
		throw new Error();
	}

	return Number(Math.round(value + "e2") + "e-2");
}


/*******************************
	isCreditCardNo
*******************************/
function isCreditCardNo(value) {
	return patterns.creditCardNo.test(value);
}


/*******************************
	isCreditCardCVC
*******************************/
function isCreditCardCVC(value) {
	return patterns.creditCardCVC.test(value);
}


/*******************************
	isCreditCardExp
*******************************/
function isCreditCardExp(value) {
	return patterns.creditCardExp.test(value);
}


/*******************************
	isLatLng
*******************************/
function isLatLng(lat, lng) {

	if (!isFinite(lat)) {
		return false;
	}

	if (lat < -90) {
		return false;
	}

	if (90 < lat) {
		return false;
	}

	if (!isFinite(lng)) {
		return false;
	}

	if (lng < -180) {
		return false;
	}

	if (180 < lng) {
		return false;
	}

	return true;
}


/*******************************
	md5
*******************************/
function md5(value) {
	return crypto.createHash("md5").update(value).digest("hex");
}


/*******************************
	encrypt
*******************************/
function encrypt(password, value) {

	const json = JSON.stringify(value);
	const cipher = crypto.createCipher("aes-256-ctr", password);
	return cipher.update(json, "utf8", "base64") + cipher.final("base64");
}


/*******************************
	decrypt
*******************************/
function decrypt(password, value) {

	const decipher = crypto.createDecipher("aes-256-ctr", password);
	const json = decipher.update(value, "base64", "utf8") + decipher.final("utf8");
	return JSON.parse(json);
}


/*******************************
	encryptHex
*******************************/
function encryptHex(password, value) {

	const json = JSON.stringify(value);
	const cipher = crypto.createCipher("aes-256-ctr", password);
	return cipher.update(json, "utf8", "hex") + cipher.final("hex");
}


/*******************************
	decryptHex
*******************************/
function decryptHex(password, value) {

	const decipher = crypto.createDecipher("aes-256-ctr", password);
	const json = decipher.update(value, "hex", "utf8") + decipher.final("utf8");
	return JSON.parse(json);
}


/*******************************
	turkishInvariant
*******************************/
function turkishInvariant(value) {

	/*
		NOTE: this file must be saved with utf-8 encoding!
	*/
	return value
		.replace(/ç/g, "c")
		.replace(/ğ/g, "g")
		.replace(/ı/g, "i")
		.replace(/ö/g, "o")
		.replace(/ş/g, "s")
		.replace(/ü/g, "u")
		.replace(/Ç/g, "C")
		.replace(/Ğ/g, "G")
		.replace(/İ/g, "I")
		.replace(/Ö/g, "O")
		.replace(/Ş/g, "S")
		.replace(/Ü/g, "U")
		;
}


/*******************************
	getPublicIP
*******************************/
function getPublicIP(cb) {

	const client = restify.createStringClient({
		url: "http://api.ipify.org"
	});

	client.get("/", (err, req, res, data) => {

		if (err) {
			return cb(err);
		}

		cb(null, data);
	});
}


/*******************************
	htmlEscape
*******************************/
function htmlEscape(value) {

	return value
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/'/g, "&apos;")
		.replace(/"/g, "&quot;")
		;
}

/*******************************
	createTable
*******************************/
function createTable(array, key) {

	if (!isArray(array)) {
		throw new Error();
	}

	if (!isString(key)) {
		throw new Error();
	}

	const result = {};

	for (let i = 0, length = array.length; i < length; i++) {

		const item = array[i];
		if (!isObject(item)) {
			throw new Error();
		}

		const id = item[key];

		if (result[id] !== undefined) {

			console.log(JSON.stringify(array, null, "  "));

			throw new Error("id (%j) is already defined.".format(id));
		}

		result[id] = item;
	}

	return result;
}

/*******************************
	differentiate
*******************************/
function differentiate(internals, internalKey, externals, externalKey, update) {

	if (!isArray(internals)) {
		throw new Error();
	}

	if (!isString(internalKey)) {
		throw new Error();
	}

	if (!isArray(externals)) {
		throw new Error();
	}

	if (!isString(externalKey)) {
		throw new Error();
	}

	if (!isFunction(update)) {
		throw new Error();
	}

	const internalsTable = createTable(internals, internalKey);
	const externalsTable = createTable(externals, externalKey);

	const difference = {
		delete: [],
		update: [],
		import: []
	};

	for (let i = 0, length = internals.length; i < length; i++) {

		const internal = internals[i];
		const internalId = internal[internalKey];
		const external = externalsTable[internalId];

		if (external === undefined) {
			difference.delete.push(internal);
		}
		else {

			let updateResult = update(internal, external);
			if (updateResult) {
				difference.update.push({
					item: internal,
					updateResult
				});
			}

			// if (update(internal, external)) {
			// 	difference.update.push(internal);
			// }
		}
	}

	for (let i = 0, length = externals.length; i < length; i++) {

		const external = externals[i];
		const externalId = external[externalKey];
		const internal = internalsTable[externalId];

		if (internal === undefined) {
			difference.import.push(external);
		}
	}

	return difference;
}

/*******************************
	renderHourOfDay
*******************************/
function renderHourOfDay(ts, tz) {
	return moment(ts).utcOffset(tz).format("HH:mm");
}

/*******************************
	run
*******************************/
function run(array, cb) {

	var error;

	function loop(index) {

		// ASSERT(Number.isInteger(index))
		// ASSERT(0 <= index)

		for (let i = index; i < array.length; i++) {

			const activity = array[i];

			// ASSERT(error === undefined)

			switch (invoke(i, activity)) {

				case 0:
					// ASSERT(error === undefined)

					return;

				case 1:
					// ASSERT(error === undefined)

					break;

				case 2:
					return cb(error);

				case 3:
					throw new Error("run is buggy.");
			}
		}

		// ASSERT(error === undefined)

		return cb();
	}

	function invoke(index, activity) {

		// ASSERT(Number.isInteger(index))
		// ASSERT(0 <= index)
		// ASSERT(error === undefined)

		/*
				[0]		->	"cb()"		->		1
				[0]		->	"cb(err)"	->		2
				[0]		->	"return"	->		3
				[0]		->	"throw"		->		(4)

				1		->	"return"	->		(5)
				1		->	"throw"		->		(6)

				2		->	"return"	->		(7)
				2		->	"throw"		->		(8)

				3		->	"cb()"		->		(9)
				3		->	"cb(err)"	->		(10)
		*/
		var state = 0;

		var reportedError;
		var thrownError;

		try {

			activity(err => {

				// ASSERT(0 <= state)
				// ASSERT(state <= 10)

				// DEBUG_STATE(state)

				switch (state) {

					case 0:
						// ASSERT(error === undefined)
						// ASSERT(reportedError === undefined)

						if (err) {
							reportedError = err;
							state = 2;

							// DEBUG_STATE(state)
						}
						else {
							state = 1;

							// DEBUG_STATE(state)
						}
						break;

					case 1:
						// ASSERT(error === undefined)
						// ASSERT(reportedError === undefined)

						throw new Error(`'${index}' already completed.`);

					case 2:
						// ASSERT(error === undefined)
						// ASSERT(reportedError)

						throw new Error(`'${index}' already failed.`);

					case 3:
						// ASSERT(reportedError === undefined)

						if (err) {
							state = 10;

							// DEBUG_STATE(state)

							cb(err);
						}
						else {
							state = 9;

							// DEBUG_STATE(state)

							loop(index + 1);
						}
						break;

					case 4:
						throw new Error(`'${index}' already failed.`);

					case 5:
						throw new Error(`'${index}' already completed.`);

					case 6:
						throw new Error(`'${index}' already failed.`);

					case 7:
						// ASSERT(error)
						// ASSERT(reportedError)
						// ASSERT(error === reportedError)

						throw new Error(`'${index}' already failed.`);

					case 8:
						// ASSERT(error)
						// ASSERT(reportedError)
						// ASSERT(error === reportedError)

						throw new Error(`'${index}' already failed.`);

					case 9:
						// ASSERT(reportedError === undefined)

						throw new Error(`'${index}' already completed.`);

					case 10:
						throw new Error(`'${index}' already failed.`);

					default:
						throw new Error("run is buggy.");
				}
			});
		}
		catch (e) {

			// ASSERT(0 <= state)
			// ASSERT(state <= 2)

			// DEBUG_STATE(state)

			switch (state) {
				case 0:
					// ASSERT(error === undefined)
					// ASSERT(reportedError === undefined)

					state = 4;

					// DEBUG_STATE(state)

					error = e;
					return 2;

				case 1:
					// ASSERT(error === undefined)
					// ASSERT(reportedError === undefined)

					state = 6;

					// DEBUG_STATE(state)

					error = e;
					return 2;

				case 2:
					// ASSERT(error === undefined)
					// ASSERT(reportedError)

					state = 8;

					// DEBUG_STATE(state)

					error = reportedError;
					return 2;

				default:
					throw new Error("run is buggy.");
			}
		}

		// ASSERT(0 <= state)
		// ASSERT(state <= 2)

		// DEBUG_STATE(state)

		switch (state) {
			case 0:
				// ASSERT(error === undefined)
				// ASSERT(reportedError === undefined)

				state = 3;

				// DEBUG_STATE(state)

				return 0;

			case 1:
				// ASSERT(error === undefined)
				// ASSERT(reportedError === undefined)

				state = 5;

				// DEBUG_STATE(state)

				return 1;

			case 2:
				// ASSERT(error === undefined)
				// ASSERT(reportedError)

				state = 7;

				// DEBUG_STATE(state)

				error = reportedError;
				return 2;

			default:
				throw new Error("run is buggy.");
		}
	}

	return loop(0);
}

function sanity() {

	function assert(value, expected) {
		if (value !== expected) {
			throw new Error();
		}
	}

	assert(isArray([]), true);
	assert(isArray([undefined]), true);
	assert(isArray([null]), true);
	assert(isArray([0]), true);
	assert(isArray([""]), true);
	assert(isArray([{}]), true);
	assert(isArray(), false);
	assert(isArray(undefined), false);
	assert(isArray(null), false);
	assert(isArray(""), false);
	assert(isArray(new String()), false);
	assert(isArray(new String("")), false);
	assert(isArray(new String("123")), false);
	assert(isArray(new Object()), false);
	assert(isArray(function () { }), false);
	assert(isArray(() => { }), false);
	assert(isArray({}), false);

	assert(isObject({}), true);

	assert(isString(``), true);
	assert(isString(''), true);
	assert(isString(""), true);
	assert(isString(new String()), true);
}

sanity();

module.exports = {
	exportAll,
	ts,
	newid,
	isArray,
	isFinite,
	isInteger,
	isString,
	isObject,
	isFunction,
	isId,
	isEmail,
	isCode,
	isPhone,
	isCreditCardNo,
	isCreditCardCVC,
	isCreditCardExp,
	isLatLng,

	getId,
	getTrimmed,
	getEmail,
	turkishInvariant,
	htmlEscape,

	round2dec,

	md5,
	encrypt,
	encryptHex,
	decrypt,
	decryptHex,

	getPublicIP,

	createTable,
	differentiate,
	renderHourOfDay,

	run
};
