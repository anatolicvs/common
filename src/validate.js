"use strict";
const { format } = require("util");
const { isArray } = Array;
const { isInteger, isFinite } = Number;

function validate(schema, instance, name) {

	const queue = [];

	function enqueue(schema, instance, name) {

		queue.push({
			schema,
			instance,
			name
		});
	}

	function dequeue() {

		return queue.shift();
	}

	function length() {

		return queue.length;
	}

	let errors;

	function report(...args) {

		if (errors === undefined) {
			errors = [];
		}

		errors.push(
			format(...args)
		);
	}

	function reportCompound(compound) {

		if (errors === undefined) {
			errors = [];
		}

		errors.push(
			compound
		);
	}

	function validateObject(instance, name) {

		if (instance === null) {
			report(
				"%s (null) is not an object.",
				name
			);

			return;
		}

		if (typeof instance === "object") {
			// ok
		}
		else {
			report(
				"%s (%j) is not an object.",
				name,
				instance
			);

			return;
		}

		if (Array.isArray(instance)) {

			report(
				"%s (%j) is not an object.",
				name,
				instance
			);
		}
	}

	function validateSchemaObject(instance, name, schema) {

		if (instance === null) {
			report(
				"%s (null) is not an object.",
				name
			);

			return;
		}

		if (typeof instance === "object") {
			// ok
		}
		else {
			report(
				"%s (%j) is not an object.",
				name,
				instance
			);

			return;
		}

		if (Array.isArray(instance)) {

			report(
				"%s (%j) is not an object.",
				name,
				instance
			);

			return;
		}

		const { properties } = schema;

		if (properties === undefined) {
			// ok
		}
		else {
			for (const propertyName in properties) {

				const childSchema = properties[propertyName];
				const childInstance = instance[propertyName];

				enqueue(
					childSchema,
					childInstance,
					`${name}.${propertyName}`
				);
			}
		}
	}

	function validateArray(instance, name) {

		if (Array.isArray(instance)) {
			// ok
		}
		else {

			report(
				"%s (%j) is not an array.",
				name,
				instance
			);

			return;
		}
	}

	function validateSchemaArray(instance, name, schema) {

		if (Array.isArray(instance)) {
			// ok
		}
		else {

			report(
				"%s (%j) is not an array.",
				name,
				instance
			);

			return;
		}

		const { items } = schema;

		if (items === undefined) {
			// ok
		}
		else {
			for (let i = 0; i < instance.length; i++) {

				const childInstance = instance[i];

				enqueue(
					items,
					childInstance,
					`${name}[${i}]`
				);
			}
		}
	}

	function validateString(instance, name) {

		if (typeof instance === "string") {
			// ok
		}
		else {

			report(
				"%s (%j) is not a string.",
				name,
				instance
			);
		}
	}

	function validateNumber(instance, name) {

		if (typeof instance === "number") {
			// ok
		}
		else {

			report(
				"%s (%j) is not a number.",
				name,
				instance
			);

			return;
		}

		if (Number.isNaN(instance)) {

			report(
				"%s (%j) is not a number.",
				name,
				instance
			);
		}
	}

	function validateInteger(instance, name) {

		if (isInteger(instance)) {
			// ok
		}
		else {
			report(
				"%s (%j) is not an integer.",
				name,
				instance
			);

			return;
		}
	}

	function validateFinite(instance, name) {

		if (isFinite(instance)) {
			// ok
		}
		else {
			report(
				"%s (%j) is not a finite.",
				name,
				instance
			);

			return;
		}
	}

	function validateBoolean(instance, name) {

		if (instance === true) {
			// ok
		}
		else if (instance === false) {
			// ok
		}
		else {
			report(
				"%s (%j) is not a boolean.",
				name,
				instance
			);

			return;
		}
	}

	function validateConstant(instance, name) {

		if (instance === undefined) {
			// ok
		}
		else {
			report(
				"%s (%j) is not equal to undefined.",
				name,
				instance
			);

			return;
		}
	}

	function validateSchemaConstant(instance, name, schema) {

		const { value } = schema;

		if (instance === value) {
			// ok
		}
		else {
			report(
				"%s (%j) is not equal to %j.",
				name,
				instance,
				value
			);

			return;
		}
	}

	enqueue(
		schema,
		instance,
		name
	);

	do {

		const { schema, instance, name } = dequeue();

		if (typeof schema === "string") {

			if (instance === undefined) {

				report(
					"%s is required.",
					name
				);

				continue;
			}

			switch (schema) {

				case "object":
					validateObject(instance, name);
					break;

				case "array":
					validateArray(instance, name);
					break;

				case "string":
					validateString(instance, name);
					break;

				case "number":
					validateNumber(instance, name);
					break;

				case "integer":
					validateInteger(instance, name);
					break;

				case "finite":
					validateFinite(instance, name);
					break;

				case "boolean":
					validateBoolean(instance, name);
					break;

				case "constant":
					validateConstant(instance, name);
					break;

				default:
					throw new Error(format("unknown type %j.", schema));
			}
		}
		else if (isArray(schema)) {

			if (0 < schema.length) {

				let match;
				let acc;
				for (const item of schema) {

					const errors = validate(item, instance, name);
					if (errors === undefined) {
						match = true;
						break;
					}

					if (acc === undefined) {
						acc = [];
					}

					acc.push(errors);
				}

				if (match === true) {
					continue;
				}

				reportCompound(
					acc
				);

				continue;
			}

			report(
				"%s is not permitted to match anything.",
				name
			);
		}
		else {

			if (instance === undefined) {

				const { required } = schema;

				if (required === true) {
					report(
						"%s is required.",
						name
					);
				}
				else {
					// ok
				}

				continue;
			}

			const { type } = schema;

			switch (type) {

				case "object":
					validateSchemaObject(instance, name, schema);
					break;

				case "array":
					validateSchemaArray(instance, name, schema);
					break;

				case "string":
					validateString(instance, name, schema);
					break;

				case "integer":
					validateInteger(instance, name, schema);
					break;

				case "finite":
					validateFinite(instance, name, schema);
					break;

				case "boolean":
					validateBoolean(instance, name, schema);
					break;

				case "constant":
					validateSchemaConstant(instance, name, schema);
					break;

				default:
					throw new Error(format("unknown type %j %j.", schema, schema.type));
			}
		}

	} while (length());

	return errors;
}

module.exports = {
	validate
};
