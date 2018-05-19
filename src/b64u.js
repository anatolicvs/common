"use strict";

function fromBase64(b64) {

	return b64.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function toBase64(b64u) {

	const tmp = b64u.replace(/-/g, "+").replace(/_/g, "/");

	switch (tmp.length % 4) {

		case 0:
			return tmp;

		case 1:
			return `${tmp}===`;

		case 2:
			return `${tmp}==`;

		case 3:
			return `${tmp}=`;
	}
}

function fromBuffer(buffer) {

	const b64 = buffer.toString(
		"base64"
	);

	return fromBase64(
		b64
	);
}

function toBuffer(b64u) {

	const b64 = toBase64(
		b64u
	);

	return Buffer.from(
		b64,
		"base64"
	);
}

function fromObject(object) {

	const json = JSON.stringify(
		object
	);

	const buffer = Buffer.from(
		json,
		"utf8"
	)

	return fromBuffer(
		buffer
	);
}

function toObject(b64u) {

	const buffer = toBuffer(
		b64u
	);

	const json = buffer.toString(
		"utf8"
	);

	return JSON.parse(
		json
	);
}

module.exports = {
	fromBase64,
	toBase64,

	fromBuffer,
	toBuffer,

	fromObject,
	toObject
};
