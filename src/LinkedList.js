"use strict";

class LinkedListNode {
	constructor(value) {
		this.value = value;
	}
}

LinkedListNode.prototype.value = null;
LinkedListNode.prototype.prev = null;
LinkedListNode.prototype.next = null;

class LinkedList {

	addLast(value) {

		const count = this.count;

		const node = new LinkedListNode(
			value
		);

		if (this.first === null) {

			this.first = node;
			this.last = node;
			this.count = 1;
		}
		else {

			const last = this.last;
			node.prev = last;
			last.next = node;
			this.last = node;
			this.count = count + 1;
		}

		return node;
	}

	addLastNode(node) {

		const count = this.count;

		if (this.first === null) {

			node.prev = null;
			node.next = null;
			this.first = node;
			this.last = node;
			this.count = 1;
		}
		else {

			const last = this.last;
			node.prev = last;
			node.next = null;
			last.next = node;
			this.last = node;
			this.count = count + 1;
		}
	}

	remove(node) {

		const count = this.count;
		if (0 < count) {

		}
		else {
			throw new Error();
		}

		const prev = node.prev;
		const next = node.next;

		if (prev === null) {

			if (next === null) {

				// assert(count === 1);
				// assert(this.first === node);
				// assert(this.last === node);

				this.first = null;
				this.last = null;
				this.count = 0;
			}
			else {

				// assert(1 < count);
				// assert(next.prev === node);
				// assert(this.first === node);
				// assert(this.last !== node);

				next.prev = null;
				this.first = next;
				this.count = count - 1;
			}
		}
		else {

			if (next === null) {

				// assert(1 < count);

				prev.next = null;
				this.last = prev;
				this.count = count - 1;
			}
			else {

				// assert(1 < count);

				next.prev = prev;
				prev.next = next;
				this.count = count - 1;
			}
		}
	}

	clear() {
		this.count = 0;
		this.first = null;
		this.last = null;
	}
}

LinkedList.prototype.count = 0;
LinkedList.prototype.first = null;
LinkedList.prototype.last = null;

module.exports = {
	LinkedListNode,
	LinkedList
};
