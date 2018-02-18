"use strict";
const crypto = require("crypto");

function padString(s) {

	switch (s.length % 4) {

		case 0:
			return s;

		case 1:
			return `${s}===`;

		case 2:
			return `${s}==`;

		case 3:
			return `${s}=`;
	}
}

function b64Tob64u(b64) {
	return b64.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function b64uTob64(b64u) {

	return padString(
		b64u.replace(/-/g, "+").replace(/_/g, "/")
	);
}

function bufferTob64u(buffer) {

	const b64 = buffer.toString(
		"base64"
	);

	return b64Tob64u(
		b64
	);
}

function objectTob64u(object) {

	const json = JSON.stringify(
		object
	);

	const buffer = Buffer.from(
		json,
		"utf8"
	)

	return bufferTob64u(
		buffer
	);
}

function b64uToBuffer(b64u) {

	const b64 = b64uTob64(
		b64u
	);

	return Buffer.from(
		b64,
		"base64"
	);
}

function b64uToObject(b64u) {

	const buffer = b64uToBuffer(
		b64u
	);

	const json = buffer.toString(
		"utf8"
	);

	return JSON.parse(
		json
	);
}

function hmacsha256(secret, string) {

	const hmac = crypto.createHmac(
		"sha256",
		secret
	);

	hmac.update(
		string,
		"utf8"
	);

	return hmac.digest();
}

function rsasha256sign(privateKey, string) {

	const sign = crypto.createSign(
		"RSA-SHA256"
	);

	sign.update(
		string,
		"utf8"
	);

	return sign.sign(
		privateKey
	);
}

function rsasha256verify(publicKey, string, signature) {

	const verify = crypto.createVerify(
		"RSA-SHA256"
	);

	verify.update(
		string,
		"utf8"
	);

	return verify.verify(
		publicKey,
		signature
	);
}

class JWTService {

	createHeader() {

		switch (this.defaultAlgorithm) {

			case "HS256":
				return {
					alg: "HS256",
					kid: this.defaultSecret
				};

			case "RS256":
				return {
					alg: "RS256",
					kid: this.defaultPrivateKey
				};

			default:
				throw new Error();
		}
	}

	encode(payload) {

		const header = this.createHeader();

		const headerString = objectTob64u(
			header
		);

		const payloadString = objectTob64u(
			payload
		);

		let signature;
		switch (header.alg) {

			case "HS256": {

				const kid = header.kid;
				const secret = this.secrets[kid];

				signature = hmacsha256(
					secret,
					`${headerString}.${payloadString}`
				)

				break;
			}

			case "RS256": {

				const kid = header.kid;
				const privateKey = this.privateKeys[kid];

				signature = rsasha256sign(
					privateKey,
					`${headerString}.${payloadString}`
				)

				break;
			}

			default:
				throw new Error();
		}

		const signatureString = bufferTob64u(
			signature
		);

		const token = `${headerString}.${payloadString}.${signatureString}`;

		return {
			header,
			headerString,
			payloadString,
			signature,
			signatureString,
			token
		};
	}

	decode(token) {

		if (typeof token === "string") {
			// ok
		}
		else {
			throw new Error("jwt::invalid-token");
		}

		if (0 < token.length) {
			// ok
		}
		else {
			throw new Error("jwt::invalid-token");
		}

		const parts = token.split(".");

		if (parts.length !== 3) {
			throw new Error("jwt::invalid-token");
		}

		const headerString = parts[0];
		const payloadString = parts[1];
		const signatureString = parts[2];

		if (0 < headerString.length) {
			// ok
		}
		else {
			throw new Error("jwt::invalid-token");
		}

		if (0 < payloadString.length) {
			// ok
		}
		else {
			throw new Error("jwt::invalid-token");
		}

		if (0 < signatureString.length) {
			// ok
		}
		else {
			throw new Error("jwt::invalid-token");
		}

		let header;

		try {
			header = b64uToObject(
				headerString
			);
		}
		catch (error) {
			throw new Error("jwt::invalid-token");
		}

		let payload;
		try {
			payload = b64uToObject(
				payloadString
			);
		}
		catch (error) {
			throw new Error("jwt::invalid-token");
		}

		let signature;
		try {
			signature = b64uToBuffer(
				signatureString
			);
		}
		catch (error) {
			throw new Error("jwt::invalid-token");
		}

		let verified;
		switch (header.alg) {

			case "HS256": {

				const kid = header.kid;
				const secret = this.secrets[kid];

				const calculatedSignature = hmacsha256(
					secret,
					`${headerString}.${payloadString}`
				);

				verified = Buffer.compare(signature, calculatedSignature) === 0;
				break;
			}

			case "RS256": {

				const kid = header.kid;
				const publicKey = this.publicKeys[kid];

				verified = rsasha256verify(
					publicKey,
					`${headerString}.${payloadString}`,
					signature
				);
				break;
			}

			default: {
				throw new Error("jwt::invalid-token");
			}
		}

		return {
			header,
			payload,
			signature,
			verified
		}
	}
}

JWTService.prototype.defaultAlgorithm = null;
JWTService.prototype.defaultPrivateKey = null;
JWTService.prototype.defaultSecret = null;
JWTService.prototype.secrets = null;
JWTService.prototype.privateKeys = null;
JWTService.prototype.publicKeys = null;

module.exports = {
	JWTService
};
