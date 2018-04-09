"use strict";
const crypto = require("crypto");
const assert = require("assert");
const {
	EventEmitter
} = require("events");

/*
 * Symbolic Constants from the RFC:
 */
const MAGIC_WEBSOCKET_UUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const NONCE_LENGTH = 16;
const OPCODE = {
	CONT: 0x0,
	TEXT: 0x1,
	BINARY: 0x2,
	CLOSE: 0x8,
	PING: 0x9,
	PONG: 0xA
};

const CLOSECODE = {
	NORMAL: 1000,
	GOING_AWAY: 1001,
	PROTOCOL_ERROR: 1002,
	UNACCEPTABLE: 1003,
	MALFORMED: 1007,
	POLICY_VIOLATION: 1008,
	TOO_BIG: 1009,
	MISSING_EXTENSION: 1010,
	UNEXPECTED_ERROR: 1011
};

function sha1(value) {
	return crypto.createHash("sha1").update(value).digest("base64");
}

function _findCloseCode(code) {

	const keys = Object.keys(CLOSECODE);
	for (var i = 0; i < keys.length; i++) {

		const key = keys[i];
		if (CLOSECODE[key] === code)
			return key;
	}

	return null;
}

class WebSocket {

	static generateKey() {

		return crypto.randomBytes(NONCE_LENGTH).toString("base64");
	}

	static mask(data, mask) {

		const dataLength = data.length;
		const maskLength = mask.length;

		for (let i = 0; i < dataLength; i++) {

			data[i] = data[i] ^ mask[i % maskLength];
		}
	}

	static accept(request, socket, head) {

		/*
		 * Return any potential parse overrun back to the
		 * front of the stream:
		 */
		if (head && head.length > 0) {
			socket.unshift(head);
		}

		/*
		 * Check for the requisite headers in the Upgrade request:
		 */
		const upgrade = request.headers["upgrade"];

		if (!upgrade || upgrade.toLowerCase() !== "websocket") {
			throw new Error("Missing Upgrade Header");
		}

		const wskey = request.headers["sec-websocket-key"];
		if (!wskey) {
			throw new Error("Missing Sec-WebSocket-Key Header");
		}

		const wsver = request.headers["sec-websocket-version"];
		if (wsver && wsver !== "13") {
			throw new Error("Unsupported Sec-WebSocket-Version");
		}

		/*
		 * Write the response that lets the client know we've accepted the
		 * Upgrade to WebSockets:
		 */

		function _generateResponse(wskey) {

			const wsaccept = sha1(wskey + MAGIC_WEBSOCKET_UUID);

			return ([
				"HTTP/1.1 101",
				"Upgrade: websocket",
				"Connection: Upgrade",
				"Sec-WebSocket-Accept: " + wsaccept
			].join("\r\n") + "\r\n\r\n");
		}

		socket.write(_generateResponse(wskey));

		const options = {
			remoteMustMask: true,
			localMustMask: false
		};

		return new WebSocketConnection(options, socket);
	}

	static connect(response, socket, head, key, detached) {

		/*
		 * Return any potential parse overrun back to the
		 * front of the stream:
		 */
		if (head && head.length > 0) {
			socket.unshift(head);
		}

		/*
		 * Check for the requisite headers in the Upgrade response:
		 */
		const connection = response.headers["connection"];
		if (!connection || connection.toLowerCase() !== "upgrade") {
			throw new Error("Missing Connection Header");
		}

		const upgrade = response.headers["upgrade"];
		if (!upgrade || upgrade.toLowerCase() !== "websocket") {
			throw new Error("Missing Upgrade Header");
		}

		const wsaccept = response.headers["sec-websocket-accept"];
		if (!wsaccept || wsaccept !== sha1(key + MAGIC_WEBSOCKET_UUID)) {
			throw new Error("Missing Sec-WebSocket-Accept Header");
		}

		const wsver = response.headers["sec-websocket-version"];

		if (wsver && wsver !== "13") {
			throw new Error("Unsupported Sec-WebSocket-Version");
		}

		if (detached === true) {
			return socket;
		}

		const options = {
			remoteMustMask: false,
			localMustMask: true
		};

		return new WebSocketConnection(options, socket);
	}
}

class WebSocketConnection extends EventEmitter {

	constructor(options, socket) {

		super();

		this._data = new Buffer(0);

		this._close_written = false;
		this._close_received = false;
		this._end_emitted = false;

		this._close_code = null;
		this._close_reason = null;

		this._options = options;
		this._socket = socket;

		socket.on("data", this.onData.bind(this));
		socket.on("end", this.onEnd.bind(this));
		socket.on("error", this.onError.bind(this));
	}

	onData(chunk) {

		this._data = Buffer.concat([this._data, chunk]);

		while (!this._end_emitted) {

			if (this.readFrame()) {

			} else {
				break;
			}
		}
	}

	onError(error) {

		console.log(error);
		if (this._end_emitted) {
			return;
		}

		this._end_emitted = true;

		/*
		 * Unfortunately, in the case of a write-after-end error there
		 * is no error code set. In this case we check that the error
		 * message property is equal to the string "write after end" as
		 * it is specified in the node runtime.
		 */
		if (error.code === "ECONNRESET" || error.code === "EPIPE" || error.message === "write after end") {

			/*
			 * Treat end-of-stream errors as merely an end
			 * of stream.  If we received a CLOSE frame, it
			 * was a graceful end.  If we did not, it was not.
			 */
			if (this._close_received) {
				// ok
			}
			else {
				this.emit(
					"connectionReset"
				);
			}

			this.emit(
				"end",
				this._close_code,
				this._close_reason
			);

			return;
		}

		this.emit(
			"error",
			error
		);

		this.emit(
			"end"
		);
	}

	onEnd() {

		console.log("onEnd");
		/*
		 * If we did not receive a CLOSE frame, then the connection was
		 * terminated prematurely.
		 */

		if (this._close_received) {
			// ok
		}
		else {
			this.emit(
				"connectionReset"
			);
		}

		if (this._end_emitted) {

			return;
		}

		this._end_emitted = true;

		this.emit(
			"end",
			this._close_code,
			this._close_reason
		);
	}

	readFrame() {

		if (this._data.length < 2) {
			return false;
		}

		const w0 = this._data.readUInt16BE(
			0
		);

		let position = 2;

		const fin = !!(w0 & (1 << 15));
		const opcode = (w0 & 0x0f00) >> 8;
		const mask = !!(w0 & (1 << 7));
		const len0 = w0 & 0x007f;
		const maskbytes = [];

		if (this._options.remoteMustMask) {

			if (mask) {
				// ok
			}
			else {

				this._end_emitted = true;

				this.emit(
					"error",
					new Error("not-masked")
				);

				console.log("end 1");
				this.emit(
					"end"
				);

				this._socket.end();

				return false;
			}
		}

		if (fin === true) {
			// ok
		}
		else {

			/*
			 * XXX We should handle multi-part messages:
			 */

			this.end();
			return false;
		}

		/*
		 * Determine the payload length; this may be in the common bytes, or in
		 * an additional field.
		 */

		let len;

		if (len0 < 126) {

			len = len0;
		}
		else if (len0 === 126) {

			if (this._data.length < position + 2) {
				return false;
			}

			len = this._data.readUInt16BE(
				position
			);

			position += 2;
		}
		else {

			if (this._data.length < position + 4 + 4) {
				return false;
			}

			len = this._data.readUInt32BE(
				position
			);

			position += 4;
			/*
			 * XXX We cannot usefully use a 64-bit value, so make sure the
			 * upper 32 bits are zero for now.
			 */
			if (len === 0) {
				// ok
			}
			else {

				this._end_emitted = true;

				this.emit(
					"error",
					new Error("frame-too-long")
				);

				console.log("end 2");
				this.emit(
					"end"
				);

				this._socket.end();

				return false;
			}

			len = this._data.readUInt32BE(
				position
			);

			position += 4;
		}

		/*
		 * Read the remote connection's mask key:
		 */
		if (mask) {

			if (this._data.length < position + 4) {
				return false;
			}

			for (let i = 0; i < 4; i++) {

				maskbytes.push(
					this._data.readUInt8(
						position
					)
				);

				position++;
			}
		}

		if (this._data.length < position + len) {
			return false;
		}

		const payload = this._data.slice(
			position,
			position + len
		);

		position += len;

		this._data = this._data.slice(
			position
		);

		if (mask) {

			WebSocket.mask(
				payload,
				maskbytes
			);
		}

		switch (opcode) {

			case OPCODE.TEXT: {

				const stringOut = payload.toString("utf8");

				this.emit(
					"text",
					stringOut
				);

				break;
			}

			case OPCODE.BINARY: {

				this.emit(
					"binary",
					payload
				);

				break;
			}

			case OPCODE.PING: {

				this.emit(
					"ping",
					payload
				);

				break;
			}

			case OPCODE.PONG: {

				this.emit(
					"pong",
					payload
				);

				break;
			}

			case OPCODE.CLOSE: {

				/*
				 * We've received a CLOSE frame, either as a result of a
				 * remote-initiated CLOSE, or in response to a CLOSE frame we
				 * sent.  In the former case, the RFC dictates that we respond
				 * in kind; otherwise close the socket.
				 */

				this._close_received = true;

				if (payload.length < 2) {
					// ok
				}
				else {

					this._close_code = _findCloseCode(
						payload.readUInt16BE(0)
					);

					this._close_reason = payload.toString("utf8", 2);
				}

				this.end();
				this._socket.end();
				break;
			}
		}

		return true;
	}

	send(data) {

		if (typeof data === "string") {

			this.writeFrame(
				OPCODE.TEXT,
				Buffer.from(data, "utf8")
			);
		}
		else if (Buffer.isBuffer(data)) {

			this.writeFrame(
				OPCODE.BINARY,
				data
			);
		}
		else {

			throw new Error();
		}
	}

	ping() {

		const nonce = this.randomBytes(2);

		this.writeFrame(
			OPCODE.PING,
			nonce
		);
	}

	pong() {

		const nonce = this.randomBytes(2);

		this.writeFrame(
			OPCODE.PONG,
			nonce
		);
	}

	end(reason) {

		if (this._close_written) {
			return;
		}

		this._close_written = true;

		let buffer;

		if (reason === undefined) {

			buffer = new Buffer(2);

			buffer.writeUInt16BE(
				CLOSECODE.NORMAL,
				0
			);
		}
		else if (typeof reason === "string") {

			buffer = new Buffer(2 + Buffer.byteLength(reason, "utf8"));

			buffer.writeUInt16BE(
				CLOSECODE.NORMAL,
				0
			);

			buffer.write(
				reason,
				2
			);
		}
		else {
			throw new Error();
		}

		this.writeFrame(
			OPCODE.CLOSE,
			buffer
		);
	}

	/*
	 * Public: WatershedConnection.destroy()
	 *
	 * Immediately destroy the underlying socket, without sending a CLOSE
	 * frame.
	 */
	destroy() {

		if (this._socket === null) {
			// ok
		}
		else {

			this._socket.removeAllListeners();
			this._socket.destroy();
			this._socket = null;
		}

		if (this._end_emitted) {
			// ok
		}
		else {
			this.emit("end", this._close_code, this._close_reason);
			this._end_emitted = true;
		}
	}

	writeFrame(opcode, data) {

		assert(Buffer.isBuffer(data));

		let maskbuf = null;
		var hdr;
		var obj = {
			fin: true,
			opcode: opcode
		};

		/*
		 * According to the RFC, the client MUST mask their outgoing frames.
		 */
		if (this._options.localMustMask) {

			maskbuf = this.randomBytes(4);

			WebSocket.mask(
				data,
				maskbuf
			);
		}

		/*
		 * Construct the type of payload length we need:
		 */
		if (data.length <= 125) {

			hdr = new Buffer(2);
			obj.len0 = data.length;
		}
		else if (data.length <= 0xffff) {

			hdr = new Buffer(2 + 2);
			obj.len0 = 126;
			hdr.writeUInt16BE(data.length, 2);
		}
		else if (data.length <= 0xffffffff) {

			hdr = new Buffer(2 + 8);
			obj.len0 = 127;
			hdr.writeUInt32BE(0, 2);
			hdr.writeUInt32BE(data.length, 6);
		}
		else {
			throw new Error("Frame payload must have length less than 32-bits");
		}

		/*
		 * Construct the common (first) two bytes of the header:
		 */
		let w0 = 0;

		if (obj.fin) {
			w0 = 0x8000;
		}
		else {
			// ok
		}

		w0 |= (obj.opcode << 8) & 0x0f00;
		w0 |= obj.len0 & 0x007f;

		if (maskbuf === null) {
			// ok
		}
		else {

			w0 |= 0x80;
		}

		hdr.writeUInt16BE(
			w0,
			0
		);

		this._socket.write(
			hdr
		);

		if (maskbuf === null) {
			// ok
		}
		else {

			this._socket.write(
				maskbuf
			);
		}

		this._socket.write(
			data
		);
	}
}

WebSocketConnection.prototype.randomBytes = crypto.randomBytes;

module.exports = {
	WebSocket,
	WebSocketConnection
};
