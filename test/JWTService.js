"use strict";
const crypto = require("crypto");
const tools = require("../src/tools");
const { JWTService } = require("../src/JWTService");

function isObject(value) {

	if (value === undefined) {
		return false;
	}

	if (value === null) {
		return false;
	}

	if (typeof value === "object") {
		// ok
	}
	else {
		return false;
	}

	if (Array.isArray(value) === true) {
		return false;
	}

	return true;
}

function isObjectObject(value) {

	if (isObject(value) === true) {
		// ok
	}
	else {
		return false;
	}

	if (Object.prototype.toString.call(value) === "[object Object]") {
		return true;
	}

	return false;
}

const publicKey = [
	"-----BEGIN PUBLIC KEY-----",
	"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtHJJqTPTTm8U56NFbwfo",
	"CqoIAwCSzvJn9tipY8klvGQENp2g1Drs600PSNiDrzOWBY/ahGFQixmbuBeHSO2P",
	"sdgdGs0ChKNBBC2Ow5GzSaDHC6OZbGDlPHvtnFkJL2WUm4ZcsO0wnllQaCq66loM",
	"VBXEAsY8fYdf+kNkmfa3lJ6ybJ1mJw7cryiupqZ/8Tl+N4MZruc4f7RlXfH4ogew",
	"vxIeGlbBqWgUV8K4nsLDvT348mWCnozPDZFc1Xhfj/8YpX2spfbuy/wr1nU+HYUS",
	"3K2dYgpMY+eo2nxJRoKQPg6Z+BrUaxY2mlq0QEHwKAo1cMGX+gtKWKeBn6ECOYrS",
	"zQIDAQAB",
	"-----END PUBLIC KEY-----",
].join("\n");

const privateKey = [
	"-----BEGIN PRIVATE KEY-----",
	"MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0ckmpM9NObxTn",
	"o0VvB+gKqggDAJLO8mf22KljySW8ZAQ2naDUOuzrTQ9I2IOvM5YFj9qEYVCLGZu4",
	"F4dI7Y+x2B0azQKEo0EELY7DkbNJoMcLo5lsYOU8e+2cWQkvZZSbhlyw7TCeWVBo",
	"KrrqWgxUFcQCxjx9h1/6Q2SZ9reUnrJsnWYnDtyvKK6mpn/xOX43gxmu5zh/tGVd",
	"8fiiB7C/Eh4aVsGpaBRXwriewsO9PfjyZYKejM8NkVzVeF+P/xilfayl9u7L/CvW",
	"dT4dhRLcrZ1iCkxj56jafElGgpA+Dpn4GtRrFjaaWrRAQfAoCjVwwZf6C0pYp4Gf",
	"oQI5itLNAgMBAAECggEBAI9XnpZP+hL7gPLFq5mZAUa/bV/dK8JDpDzePilrl5OB",
	"LCuqqiENsjj1XSfz/x/Fbe57KQ4yNAZb0Gy8HBbdyGFxmSU6KD7vZO2Jtzg6XN8n",
	"Xhcr3evPSSr0E3w46mgBXLzzTlyp/w47SvmEwDj8UYDWTYAas+DQEnk+4gLAj3L6",
	"DKHwMWW9s29+ISSTECfGM+lXWmnjJuB2AbpbeUfTVyZBRbJlgyM7gGG4xxrOxqpn",
	"ejdtgdAARHUhDQv/PyN5y1AIOlobQCP1kIjqfAv6owj6p9ZlhWgQ6hlQMFsWKT3u",
	"Er9ytcRZagjjbcurtQQj9TbJwwrHGSPzAGGSkQqkDaECgYEA2sgyngXQWqFvhb04",
	"fXWxd+DyLAsBEpZFJ72qXD2TTIs8IfFlCYWTrXLkIe52gxldCs7ZLFUOBBO854gG",
	"Xm1P2PPrAFDKuJpQDgu+efMi1Pjk4jLEB7jzmEpNw4qhLD32tLLtMskUqqAKQrYj",
	"9656skYeW9iRgKutKQBIVbj6yMkCgYEA0ySaNSHVcaYE9kARF/PKzaiYaeO81sUJ",
	"hTXiB+rvYNsU0E3Y+4w0zYCdV3qMHv/9LBUpWHVCSb3+XNazYmGs9VhEXPc4OndV",
	"FmEM+0vQLEwrlbHhoGwAGzuO8Ay36916lmnohcBcFRctMqMGk1nkrGoIHUMdRiHu",
	"NCciGTkN/+UCgYBYP3aDXS78z69PT9LwcD/Ebhzg/RNSrwB4Fj4YdNHshyEbQ+aQ",
	"X6wAw54XvbtaCCoKiQL6qdg8dsW00p5XClqx1TmOaAhNTBlMgRo00IjVRQv13apK",
	"vySq4hXZ8Rov4VKY8q9Q0+EdLe7Vl+iMKHnfXP8z5vhR4W24Yx/KTBIEWQKBgQDK",
	"Io9y8Eob/Blh+iGjMaTk0FPhg8HkRkv/+H7En7i9m99IME7bOvXOCyQWfF6qyt2A",
	"F142rkNgv4BFNtITDHl9hBwmeBKffmC6BP1dks3fqqhLLjlX0C0l5RXJYBOvvZ+/",
	"YfLWITrGgiKmCIaiCHwwR24vPXJIEvGtgg1V4lYxtQKBgH+89/cBNmPKz1HUq2D2",
	"XT376f4S/Xi+/LXg3ElwNNzb5vfRDjKPGLP7zhMC9ai6nrGvO8uKIiMTWxtEigTZ",
	"eTNS3QBFekuzmEIMCeMDoY59yO+pIV0yXi1HNDYGtozipbgBy/T9pUvRPOVDqART",
	"eRdzI0+u/isd4BPbV+1a45Wo",
	"-----END PRIVATE KEY-----"
].join("\n");

const secret = "shhhh....";
const payloads = [

	null,
	true,
	false,
	123.456,
	"abc",
	{
		test: 123,
		inner: {
			go: "123",
			to: null,
			flag: false
		}
	}
];

const scenarios = [
	{
		description: "rs256",
		encode: {
			algorithm: "RS256",
			privateKeyId: "key-1",
			privateKey
		},
		decode: {
			algorithm: "RS256",
			publicKeys: {
				"key-1": publicKey
			}
		}
	},
	{
		description: "hs256",
		encode: {
			algorithm: "HS256",
			secretId: "secret-1",
			secret
		},
		decode: {
			algorithm: "HS256",
			secrets: {
				"secret-1": secret
			}
		}
	}
];


describe("JWTService", () => {

	const service = new JWTService();

	for (const scenario of scenarios) {

		describe(scenario.description, () => {

			for (const key in scenario.config) {

				service[key] = scenario.config[key];
			}

			for (const payload of payloads) {

				it(tools.format("should handle %j", payload), () => {

					delete scenario.encode.payload;
					scenario.encode.payload = payload;

					const encodeResponse = service.encode(
						scenario.encode
					);

					delete scenario.decode.token;
					scenario.decode.token = encodeResponse.token;

					const decodeResponse = service.decode(
						scenario.decode
					);

					if (!decodeResponse.verified) {
						throw new Error();
					}

					if (JSON.stringify(payload) !== JSON.stringify(decodeResponse.payload)) {
						throw new Error();
					}
				});
			}

		});
	}
});
