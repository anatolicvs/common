"use strict";
const { format } = require("util");
const { isArray } = Array;
const { isInteger, isFinite, isNaN } = Number;

const regexpCache = {};

const coderegex = /^(?=.{1,1024}$)([0-9a-zçğıöşü]+([-_.][0-9a-zçğıöşü]+)*)$/;
const trimmedregex = /^\S+($|[\s\S]*\S$)/;

const aliases = {
	"object": {
		type: "object",
		required: true
	},
	"oobject": {
		type: "object"
	},
	"array": {
		type: "array",
		required: true
	},
	"oarray": {
		type: "array"
	},
	"string": {
		type: "string",
		required: true
	},
	"ostring": {
		type: "string"
	},
	"trimmed": {
		type: "string",
		required: true,
		pattern: trimmedregex.source,
		patternName: "trimmed"
	},
	"otrimmed": {
		type: "string",
		pattern: trimmedregex.source,
		patternName: "trimmed"
	},
	"code": {
		type: "string",
		required: true,
		pattern: coderegex.source,
		patternName: "code"
	},
	"ocode": {
		type: "string",
		pattern: coderegex.source,
		patternName: "code"
	},
	"number": {
		type: "number",
		required: true
	},
	"onumber": {
		type: "number"
	},
	"integer": {
		type: "number",
		required: true,
		set: "integer"
	},
	"ointeger": {
		type: "number",
		set: "integer"
	},
	"finite": {
		type: "number",
		required: true,
		set: "finite"
	},
	"ofinite": {
		type: "number",
		set: "finite"
	},
	"boolean": {
		type: "boolean",
		required: true
	},
	"oboolean": {
		type: "boolean"
	}
};

function validate(schema, instance, name) {

	const queue = [];

	function enqueue(parency, schema, instance, name) {

		queue.push({
			parency,
			schema,
			instance,
			name
		});
	}

	function dequeue() {

		return queue.shift();
	}

	function undequeue(parency, schema, instance, name) {

		queue.unshift({
			parency,
			schema,
			instance,
			name
		});
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
					{ type: "property", instance, propertyName },
					childSchema,
					childInstance,
					`${name}.${propertyName}`
				);
			}
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
					{ type: "item", instance, index: i },
					items,
					childInstance,
					`${name}[${i}]`
				);
			}
		}
	}

	function validateSchemaString(instance, name, schema) {

		if (typeof instance === "string") {
			// ok
		}
		else {

			report(
				"%s (%j) is not a string.",
				name,
				instance
			);

			return;
		}

		const pattern = schema.pattern;
		if (pattern === undefined) {
			// ok
		}
		else if (typeof pattern === "string") {
			// ok
		}
		else {
			throw new Error();
		}

		if (pattern === undefined) {
			// ok
		}
		else {

			let regexp = regexpCache[pattern];
			if (regexp === undefined) {

				regexp = new RegExp(
					pattern
				);

				regexpCache[pattern] = regexp;
			}

			if (regexp.test(instance)) {
				// ok
			}
			else {

				if (typeof schema.patternName === "string") {

					report(
						"%s (%j) is not a %j.",
						name,
						instance,
						schema.patternName
					);
				}
				else {

					report(
						"%s (%j) does not match pattern (%s).",
						name,
						instance,
						pattern
					);
				}

				return;
			}
		}
	}

	function validateSchemaNumber(instance, name, schema) {

		const set = schema.set;
		if (set === undefined) {

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

			if (isNaN(instance)) {

				report(
					"%s (%j) is not a number.",
					name,
					instance
				);

				return;
			}

			return;
		}

		switch (set) {

			case "integer":

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

				break;

			case "finite":

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

				break;

			default:
				throw new Error();
		}
	}

	function validateSchemaInteger(instance, name, schema) {

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

	function validateSchemaFinite(instance, name, schema) {

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

	function validateSchemaBoolean(instance, name, schema) {

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

	function loop() {

		do {

			const { parency, schema, instance, name } = dequeue();

			if (typeof schema === "string") {

				const defaultSchema = aliases[schema];

				if (defaultSchema === undefined) {
					throw new Error(format("unknown type %j.", schema));
				}

				undequeue(
					parency,
					defaultSchema,
					instance,
					name
				);
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

					if (schema.required === true) {

						report(
							"%s is required.",
							name
						);
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
						validateSchemaString(instance, name, schema);
						break;

					case "number":
						validateSchemaNumber(instance, name, schema);
						break;

					case "boolean":
						validateSchemaBoolean(instance, name, schema);
						break;

					case "constant":
						validateSchemaConstant(instance, name, schema);
						break;

					case "not": {

						const errors = validate(schema.schema, instance, name);
						if (errors === undefined) {
							report(
								"%s matches.",
								name
							);
						}
						break;
					}

					default:

						throw new Error(
							format(
								"unknown type %j %j.",
								schema,
								schema.type
							)
						);
				}
			}

		} while (0 < length());
	}

	enqueue(
		{ type: "root" },
		schema,
		instance,
		name
	);

	loop();

	return errors;
}

module.exports = {
	validate
};
