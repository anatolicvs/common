"use strict";

const path = require("path");
const util = require("util");
const crypto = require("crypto");
const uuid = require("uuid");
const lodash = require("lodash");
const http = require("http");
const moment = require("moment");

// http://stackoverflow.com/questions/46155/validate-email-address-in-javascript
const emailPattern = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;

/*******************************
	patterns
*******************************/

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const patterns = {
	email: emailPattern,

	uuid: UUID,
	uuid_uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
	code: /^(?=.{1,1024}$)([0-9a-zçğıöşü]+(-[0-9a-zçğıöşü]+)*)$/,
	adminUsername: /^[a-zA-Z]{3,32}$/,
	managerUsername: /^[a-zA-Z]{3,32}@[a-zA-Z]{3,32}$/,

	// https://www.thepolyglotdeveloper.com/2015/05/use-regex-to-test-password-strength-in-javascript/
	strongPassword: /^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#\$%\^&\*])(?=.{8,})/,

	phone: /^[0-9]{10}$/,
	phoneVerify: /^[1-9][0-9]{5}$/,

	hex64: /^[0-9a-fA-F]{64}$/,

	creditCardNo: /^[0-9]{16}$/,
	creditCardCVC: /^[0-9]{3}$/,
	creditCardExp: /^(0[1-9]|1[012])([0-9]{2})$/
};

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
	rng16hex
*******************************/
function rng16hex() {
	return crypto.randomBytes(16).toString("hex");
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


function isId(value) {

	if (typeof value === "string") {

		if (UUID.test(value)) {

			return true;
		}
	}

	return false;
}


function getId(value, name) {

	if (typeof value === "string") {

		if (UUID.test(value)) {

			return value;
		}
	}

	if (name) {

		throw new Error(
			util.format(
				"%s (%j) is not an id.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not an id.",
			value
		)
	);
}

function ogetId(value, name) {

	if (value === void 0) {
		return;
	}

	return getId(value, name);
}

function isCode(value) {

	if (typeof value === "string") {

		if (patterns.code.test(value)) {

			return true;
		}
	}

	return false;
}


function getCode(value, name) {

	if (typeof value === "string") {

		if (patterns.code.test(value)) {

			return value;
		}
	}

	if (name) {

		throw new Error(
			util.format(
				"%s (%j) is not a code.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not a code.",
			value
		)
	);
}

function ogetCode(value, name) {

	if (value === void 0) {
		return;
	}

	return getCode(value, name);
}

function isString(value) {

	if (typeof value === "string") {

		return true;
	}

	return false;
}

function getString(value, name) {

	if (typeof value === "string") {

		return value;
	}

	if (name) {

		throw new Error(
			util.format(
				"%s (%j) is not a string.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not a string.",
			value
		)
	);
}

function getNonEmptyString(value, name) {

	if (typeof value === "string") {

		if (0 < value.length) {
			return value;
		}
	}

	if (name) {

		throw new Error(
			util.format(
				"%s (%j) is not a non-empty string.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not a non-empty string.",
			value
		)
	);
}

function getInteger(value, name) {

	if (isInteger(value)) {
		return value;
	}

	if (name) {
		throw new Error(
			util.format(
				"%s (%j) is not an integer.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not an integer.",
			value
		)
	);
}

function ogetInteger(value, name) {

	if (value === void 0) {
		return;
	}

	return getInteger(value, name);
}

/*******************************
	getArray
*******************************/
function getArray(value, name) {

	if (isArray(value)) {
		return value;
	}

	if (name) {
		throw new Error(
			util.format(
				"%s (%j) is not an array.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not an array.",
			value
		)
	);
}

/*******************************
	getArray
*******************************/
function getNonEmptyArray(value, name) {

	if (isArray(value)) {

		if (0 < value.length) {
			return value;
		}

		if (name) {

			throw new Error(
				util.format(
					"%s (%j) is not a non-empty array.",
					name,
					value
				)
			);
		}

		throw new Error(
			util.format(
				"(%j) is not a non-empty array.",
				value
			)
		);
	}

	if (name) {
		throw new Error(
			util.format(
				"%s (%j) is not a non-empty array.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not a non-empty array.",
			value
		)
	);
}

function getObject(value, name) {

	if (isObject(value)) {
		return value;
	}

	if (name) {
		throw new Error(
			util.format(
				"%s (%j) is not an object.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not an object.",
			value
		)
	);
}

function ogetObject(value, name) {

	if (value === void 0) {
		return;
	}

	return getObject(value, name);
}

/*******************************
	asId
*******************************/
function asId(value) {

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
function getTrimmed(value, name) {

	const trimmed = asTrimmed(
		value
	);

	if (trimmed === void 0) {

		if (name) {
			throw new Error(
				util.format(
					"%s (%j) is not a trimmed.",
					name,
					value
				)
			);
		}

		throw new Error(
			util.format(
				"(%j) is not a trimmed.",
				value
			)
		);
	}

	return trimmed;
}


/*******************************
	asTrimmed
*******************************/
function asTrimmed(value) {

	if (isString(value)) {

		value = value.trim();

		if (0 < value.length) {
			return value;
		}
	}
}


/*******************************
	isEmail
*******************************/
function isEmail(value) {
	return emailPattern.test(value);
}


/*******************************
	asEmail
*******************************/
function asEmail(value) {

	if (isEmail(value)) {
		return value;
	}
}


/*******************************
	getEmail
*******************************/
function getEmail(value) {

	if (isEmail(value)) {
		return value;
	}

	if (name) {
		throw new Error(
			util.format(
				"%s (%j) is not an email.",
				name,
				value
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not an email.",
			value
		)
	);
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
	delay
*******************************/
function delay(timeout) {

	return new Promise((resolve, reject) => {
		setTimeout(resolve, timeout);
	});
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

	const request = http.get({ host: "api.ipify.org", path: "/" }, response => {

		let body = "";
		response.on("data", data => {
			body += data;
		});

		response.on("end", () => {
			cb(null, body);
		});
	});

	request.on("error", error => {
		cb(error);
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

		if (result[id] !== void 0) {

			console.log(JSON.stringify(array, null, "  "));

			throw new Error(
				util.format(
					"id (%j) is already defined.",
					id
				)
			);
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

		if (external === void 0) {
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

		if (internal === void 0) {
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

			// ASSERT(error === void 0)

			switch (invoke(i, activity)) {

				case 0:
					// ASSERT(error === void 0)

					return;

				case 1:
					// ASSERT(error === void 0)

					break;

				case 2:
					return cb(error);

				case 3:
					throw new Error("run is buggy.");
			}
		}

		// ASSERT(error === void 0)

		return cb();
	}

	function invoke(index, activity) {

		// ASSERT(Number.isInteger(index))
		// ASSERT(0 <= index)
		// ASSERT(error === void 0)

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
						// ASSERT(error === void 0)
						// ASSERT(reportedError === void 0)

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
						// ASSERT(error === void 0)
						// ASSERT(reportedError === void 0)

						throw new Error(`'${index}' already completed.`);

					case 2:
						// ASSERT(error === void 0)
						// ASSERT(reportedError)

						throw new Error(`'${index}' already failed.`);

					case 3:
						// ASSERT(reportedError === void 0)

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
						// ASSERT(reportedError === void 0)

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
					// ASSERT(error === void 0)
					// ASSERT(reportedError === void 0)

					state = 4;

					// DEBUG_STATE(state)

					error = e;
					return 2;

				case 1:
					// ASSERT(error === void 0)
					// ASSERT(reportedError === void 0)

					state = 6;

					// DEBUG_STATE(state)

					error = e;
					return 2;

				case 2:
					// ASSERT(error === void 0)
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
				// ASSERT(error === void 0)
				// ASSERT(reportedError === void 0)

				state = 3;

				// DEBUG_STATE(state)

				return 0;

			case 1:
				// ASSERT(error === void 0)
				// ASSERT(reportedError === void 0)

				state = 5;

				// DEBUG_STATE(state)

				return 1;

			case 2:
				// ASSERT(error === void 0)
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

class ValidationError extends Error {
	constructor(message) {
		super(message);
	}
}

ValidationError.prototype.name = "ValidationError";

function validateObject(value, name) {

	if (!isObject(value)) {
		throw new ValidationError(
			util.format("%j (%j) is not an object.", name, value)
		);
	}
}

function validateEmail(value, name) {

	if (!isEmail(value)) {
		throw new ValidationError(
			util.format("%j (%j) is not an email.", name, value)
		);
	}
}

function validateGetTrimmed(value, name) {

	const trimmed = getTrimmed(value);

	if (trimmed === void 0) {
		throw new ValidationError(
			util.format("%s (%j) is not a trimmed.", name, value)
		);
	}

	return trimmed;
}

function assert(value, message) {

	if (value) {
		return;
	}

	if (message) {

		throw new Error(
			util.format(
				"assertion failed: %j.",
				message
			)
		);
	}

	throw new Error(
		"assertion failed."
	);
}

function assertEqual(value, expected, name) {

	if (value === expected) {
		return;
	}

	if (name) {

		throw new Error(
			util.format(
				"%s (%j) is not as expected (%j).",
				name,
				value,
				expected
			)
		);
	}

	throw new Error(
		util.format(
			"(%j) is not as expected (%j).",
			value,
			expected
		)
	);
}

// function sanity() {

// 	function assert(value, expected) {
// 		if (value !== expected) {
// 			throw new Error();
// 		}
// 	}

// 	assert(isArray([]), true);
// 	assert(isArray([undefined]), true);
// 	assert(isArray([null]), true);
// 	assert(isArray([0]), true);
// 	assert(isArray([""]), true);
// 	assert(isArray([{}]), true);
// 	assert(isArray(), false);
// 	assert(isArray(undefined), false);
// 	assert(isArray(null), false);
// 	assert(isArray(""), false);
// 	assert(isArray(new String()), false);
// 	assert(isArray(new String("")), false);
// 	assert(isArray(new String("123")), false);
// 	assert(isArray(new Object()), false);
// 	assert(isArray(function () { }), false);
// 	assert(isArray(() => { }), false);
// 	assert(isArray({}), false);

// 	assert(isObject({}), true);

// 	assert(isString(``), true);
// 	assert(isString(''), true);
// 	assert(isString(""), true);
// 	assert(isString(new String()), true);
// }

// sanity();

/*
	http://en.wikipedia.org/wiki/Earth_radius
*/
const EARTH_MEAN_RADIUS = 6371009;

function toDegrees(radians) {
	return radians * 180 / Math.PI;
}

function toRadians(degrees) {
	return degrees * Math.PI / 180;
}

/*
	http://mathforum.org/library/drmath/view/51879.html
*/
function calculateDistance(lat1, lng1, lat2, lng2) {

	if (!isLatLng(lat1, lng1)) {

		throw new Error(
			util.format(
				"lat1,lng1 (%j,%j) is not valid.",
				lat1,
				lng1
			)
		);
	}


	if (!isLatLng(lat2, lng2)) {

		throw new Error(
			util.format(
				"lat2,lng2 (%j,%j) is not valid.",
				lat2,
				lng2
			)
		);
	}

	const dlat = toRadians(lat2 - lat1);
	const dlng = toRadians(lng2 - lng1);

	const sin_dlat_2 = Math.sin(dlat / 2);
	const sin_dlng_2 = Math.sin(dlng / 2);

	const cos_lat1 = Math.cos(toRadians(lat1));
	const cos_lat2 = Math.cos(toRadians(lat2));

	const a = sin_dlat_2 * sin_dlat_2 + cos_lat1 * cos_lat2 * sin_dlng_2 * sin_dlng_2;
	const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

	return EARTH_MEAN_RADIUS * c;
};

/*
	https://en.wikipedia.org/wiki/Z-order_curve
*/
function zindex(x, y) {

	if (!isInteger(x)) {

		throw new Error(
			util.format(
				"x (%j) is not an integer.",
				x
			)
		);
	}

	if (!isInteger(y)) {

		throw new Error(
			util.format(
				"y (%j) is not an integer.",
				y
			)
		);
	}

	var bit = 1;
	var max = Math.max(x, y);
	var result = 0;

	while (bit <= max) {
		bit <<= 1;
	}

	bit >>= 1;

	while (bit) {

		result *= 2;

		if (x & bit) {
			result += 1;
		}

		result *= 2;

		if (y & bit) {
			result += 1;
		}

		bit >>= 1;
	}

	return result;
}

function tnzindex(lat, lng) {

	if (!isLatLng(lat, lng)) {

		throw new Error(
			util.format(
				"lat,lng (%j,%j) is not valid.",
				lat,
				lng
			)
		);
	}

	return zindex(
		Math.round((lat + 90) * 100000),
		Math.round((lng + 180) * 100000)
	);
}

function sortObject(object) {

	const keys = Object.keys(object);
	keys.sort();

	const result = {};

	for (const key of keys) {

		const value = object[key];

		if (Array.isArray(value)) {
			result[key] = value;
			continue;
		}

		if (typeof value === "object") {
			result[key] = sortObject(value);
			continue;
		}

		result[key] = value;
	}

	return result;
}

function isBase64(value) {
	try {
		return btoa(atob(value)) === value;
	} catch (err) {
		return false;
	}
}


module.exports = {
	patterns,
	format: util.format,
	exportAll,
	ts,
	newid,
	rng16hex,
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
	getCode,
	getString,
	getNonEmptyString,
	getInteger,
	getArray,
	getNonEmptyArray,
	getObject,

	getTrimmed,
	getEmail,

	ogetId,
	ogetCode,
	ogetInteger,
	ogetObject,

	asId,
	asTrimmed,
	asEmail,

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

	run,
	ValidationError,
	validateObject,
	validateEmail,
	validateGetTrimmed,

	EARTH_MEAN_RADIUS,
	toDegrees,
	toRadians,
	calculateDistance,
	zindex,
	tnzindex,

	sortObject,
	assert,
	assertEqual,

	isBase64,
	delay
};
