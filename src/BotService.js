"use strict";
const https = require("https");

class BotService {

	message(text) {

		const token = this.token;

		if (token === null) {
			return;
		}

		if (typeof text === "string") {
			// ok
		}
		else {
			return;
		}

		const payload = Buffer.from(JSON.stringify({
			channel: this.channel,
			text,
			as_user: true
		}), "utf8");

		const request = https.request({
			host: "slack.com",
			method: "POST",
			path: "/api/chat.postMessage",
			headers: {
				"Authorization": `Bearer ${token}`,
				"Content-Type": "application/json; charset=utf-8",
				"Content-Length": payload.length
			}
		});

		request.on("response", response => {
			response.on("data", chunk => { });
			response.on("end", () => { });
			response.on("error", error => { });
		});

		request.on("error", error => { });

		request.end(payload);
	}
}

BotService.prototype.token = null;
BotService.prototype.channel = null;

module.exports = {
	BotService
};
