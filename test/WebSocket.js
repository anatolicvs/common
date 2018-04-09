"use strict";
const {
	deepStrictEqual
} = require("assert");

const fs = require("fs");
const {
	EventEmitter
} = require("events");

const {
	createConnection,
	createServer
} = require("net");

const { WebSocket, WebSocketConnection } = require("../src/WebSocket");

function hex(value) {

	return Buffer.from(value, "hex");
}

function utf8(value) {

	return Buffer.from(value, "utf8");
}

function mask(data, mask) {

	const dataLength = data.length;
	const maskLength = mask.length;

	for (let i = 0; i < dataLength; i++) {

		data[i] = data[i] ^ mask[i % maskLength];
	}

	return data;
}

function create() {

	const eventEmitter = new EventEmitter();

	const log = [];
	const queue = [];
	const socket = {
		eventEmitter,
		on(event, ...args) {

			log.push({
				"on": event
			});

			eventEmitter.on(event, ...args);
		},
		read(...args) {

			log.push({
				read: args
			});

			if (0 < queue.length) {
				return queue.shift();
			}

			return null;
		},

		write(...args) {

			log.push({
				write: args
			});
		}
	}

	const connection = new WebSocketConnection({
		remoteMustMask: false,
		localMustMask: true
	}, socket);

	function randomBytes(length) {
		const buffer = new Buffer(length);
		for (let i = 0; i < length; i++) {
			buffer[i] = i % 256;
		}
		return buffer;
	}

	connection.randomBytes = randomBytes;

	connection.on("text", text => {

		log.push({
			text
		});
	});

	connection.on("binary", buffer => {

		log.push({
			binary: buffer
		});
	});

	connection.on("ping", buffer => {

		log.push({
			ping: buffer
		});
	});

	connection.on("pong", buffer => {

		log.push({
			pong: buffer
		});
	});

	connection.on("connectionReset", () => {

		log.push({
			connectionReset: null
		});
	});

	connection.on("error", error => {

		log.push({
			error: error.message,
			code: error.code
		});
	});

	connection.on("end", (code, reason) => {

		log.push({
			method: "end",
			code,
			reason
		});
	});

	function read(...values) {

		for (let i = 0; i < values.length; i++) {

			const actual = log[i];
			const expected = values[i];
			deepStrictEqual(actual, expected);
		}

		log.splice(0, values.length);
	}

	function write(...buffers) {
		queue.push(...buffers);
	}

	function end() {

		if (log.length === 0) {

		}
		else {
			console.log(JSON.stringify(log, null, 4));
			throw new Error();
		}
	}

	return {
		log,
		socket,
		connection,
		randomBytes,
		read,
		write,
		end
	};
}

describe("WebSocket", () => {

	it("constructor", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		end();
	});

	it("recv", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			write,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		socket.eventEmitter.emit("data", hex("8184"));
		socket.eventEmitter.emit("data", randomBytes(4));
		socket.eventEmitter.emit("data", mask(utf8("test"), randomBytes(4)));

		read(
			{ text: "test" }
		);

		end();
	});

	it("send text", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		connection.send("test");

		read(
			{ write: [hex("8184")] },
			{ write: [randomBytes(4)] },
			{ write: [mask(utf8("test"), randomBytes(4))] },
		);

		end();
	});

	it("send buffer", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		connection.send(utf8("test"));

		read(
			{ write: [hex("8284")] },
			{ write: [randomBytes(4)] },
			{ write: [mask(utf8("test"), randomBytes(4))] },
		);

		end();
	});

	it("ping", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		connection.ping();

		read(
			{ write: [hex("8982")] },
			{ write: [randomBytes(4)] },
			{ write: [mask(randomBytes(2), randomBytes(4))] }
		);

		end();
	});

	it("pong", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		connection.pong();

		read(
			{ write: [hex("8a82")] },
			{ write: [randomBytes(4)] },
			{ write: [mask(randomBytes(2), randomBytes(4))] }
		);

		end();
	});

	it("end", () => {

		const {
			socket,
			connection,
			randomBytes,
			read,
			end
		} = create();

		read(
			{ on: "data" },
			{ on: "end" },
			{ on: "error" }
		);

		connection.end();

		read(
			{ write: [hex("8882")] },
			{ write: [randomBytes(4)] },
			{ write: [hex("03e9")] }
		);

		end();
	});
});

describe("WebSocket", () => {

	it("", async () => {

		const server = createServer({
			allowHalfOpen: true
		});

		server.on("connection", socket => {

			console.log("server::connection");

			socket.on("data", chunk => {

				console.log("server::data", chunk);
			});

			socket.on("error", error => {
				console.log("server::error", error);
			});

			socket.on("end", () => {
				console.log("server::end");
				socket.end();
			});

			socket.on("close", hadError => {
				console.log("server::close", hadError);
			});
		});

		await new Promise((resolve, reject) => {

			server.on("listening", () => {
				resolve();
			});

			server.listen(55555);
		});

		try {

			const socket = createConnection({
				port: 55555,
				allowHalfOpen: true
			});

			await new Promise((resolve, reject) => {

				socket.on("connect", () => {

					console.log("client::connect");

					const connection = new WebSocketConnection({
						localMustMask: true
					}, socket);

					connection.on("text", text => {
						console.log("text reply:", text);
					});

					connection.on("binary", chunk => {
						console.log("binary reply of length %d.", chunk.length);
					});

					connection.on("pong", () => {
						console.log("pong");
					});

					connection.on("end", () => {

						resolve();
					});

					connection.send("test");
					connection.send(new Buffer(1024));
					connection.ping();
					connection.end(); // WS FIN
					socket.end(); // TCP FIN
					//socket.destroy();
					//console.log(socket.destroyed);
				});

				socket.on("drain", () => {
					console.log("client::drain", chunk);
				});

				socket.on("data", chunk => {
					console.log("client::data", chunk);
				});

				socket.on("error", error => {
					console.log("client::error", error);
				});

				socket.on("end", () => {
					console.log("client::end");
				});

				socket.on("close", hadError => {
					console.log("client::close", hadError);
				});

			});
		}
		finally {

			await new Promise((resolve, reject) => {

				server.on("close", () => {
					resolve();
				});

				server.close();
			});
		}
	})
});