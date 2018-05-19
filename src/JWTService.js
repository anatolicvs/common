"use strict";
const {
	createHmac,
	createSign,
	createVerify
} = require("crypto");

function hmacsha256(secret, string) {

	const hmac = createHmac(
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

	const sign = createSign(
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

	const verify = createVerify(
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

const {
	fromBuffer,
	toBuffer,
	fromObject,
	toObject
} = require("./b64u");

function createHeader(algorithm, secretId, privateKeyId) {

	switch (algorithm) {

		case "HS256":

			return {
				alg: "HS256",
				kid: secretId
			};

		case "RS256":

			return {
				alg: "RS256",
				kid: privateKeyId
			};

		default:
			throw new Error();
	}
}

class JWTService {

	encode({
		algorithm,
		secretId,
		secret,
		privateKeyId,
		privateKey,
		payload
	}) {

		const header = createHeader(
			algorithm,
			secretId,
			privateKeyId
		);

		const headerString = fromObject(
			header
		);

		const payloadString = fromObject(
			payload
		);

		let signature;
		switch (header.alg) {

			case "HS256": {

				signature = hmacsha256(
					secret,
					`${headerString}.${payloadString}`
				)

				break;
			}

			case "RS256": {

				signature = rsasha256sign(
					privateKey,
					`${headerString}.${payloadString}`
				)

				break;
			}

			default:
				throw new Error();
		}

		const signatureString = fromBuffer(
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

	decode({
		token,
		secrets,
		publicKeys
	}) {

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
			header = toObject(
				headerString
			);
		}
		catch (error) {
			throw new Error("jwt::invalid-token");
		}

		let payload;
		try {
			payload = toObject(
				payloadString
			);
		}
		catch (error) {
			throw new Error("jwt::invalid-token");
		}

		let signature;
		try {
			signature = toBuffer(
				signatureString
			);
		}
		catch (error) {
			throw new Error("jwt::invalid-token");
		}

		let verified;
		switch (header.alg) {

			case "HS256": {

				if (secrets === undefined) {
					// ok
				}
				else {
					const kid = header.kid;
					const secret = secrets[kid];

					const calculatedSignature = hmacsha256(
						secret,
						`${headerString}.${payloadString}`
					);

					verified = Buffer.compare(signature, calculatedSignature) === 0;
				}
				break;
			}

			case "RS256": {

				if (publicKeys === undefined) {
					// ok
				}
				else {
					const kid = header.kid;
					const publicKey = publicKeys[kid];

					verified = rsasha256verify(
						publicKey,
						`${headerString}.${payloadString}`,
						signature
					);
				}
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

module.exports = {
	JWTService
};
