"use strict";
const {
	isArray
} = Array;

function transform(expression, context) {

	function evaluate(expression) {

		if (expression === undefined) {

			/*
				typeof undefined is "undefined".
			*/
			return undefined;
		}

		if (expression === null) {

			/*
				typeof null is "object".
			*/
			return null;
		}

		switch (typeof expression) {

			case "boolean":
				return expression;

			case "number":
				return expression;

			case "string":
				return expression;

			case "object": {

				if (isArray(expression)) {

					return evaluateArrayLiteral(
						expression
					);
				}

				let first;
				let count = 0;
				for (const key in expression) {

					count++;

					if (first === undefined) {
						first = key;
					}
					else {
						break;
					}
				}

				if (count === 1) {

					const handlers = {
						"$undefined": evaluateUndefinedExpression,
						"$boolean": evaluateBooleanExpression,
						"$number": evaluateNumberExpression,
						"$string": evaluateStringExpression,
						"$array": evaluateArrayExpression,
						"$object": evaluateObjectExpression,
						"$lookup": evaluateLookupExpression,
						"$concat": evaluateConcatExpression,
						"$add": evaulateAddExpression
					};

					const handler = handlers[
						first
					];

					if (handler === undefined) {
						// ok
					}
					else {

						const content = expression[
							first
						];

						return handler(
							content
						);
					}
				}

				return evaluateObjectLiteral(
					expression
				);
			}

			default:
				throw new Error();
		}
	}

	function expectUndefined(value) {

		if (value === undefined) {
			// ok
		}
		else {
			throw new Error();
		}
	}

	function expectBoolean(value) {

		if (value === true) {
			// ok
		}
		else if (value === false) {
			// ok
		}
		else {
			throw new Error();
		}
	}

	function expectNumber(value) {

		if (typeof value === "number") {
			// ok
		}
		else {
			throw new Error();
		}
	}

	function expectString(value) {

		if (typeof value === "string") {
			// ok
		}
		else {
			throw new Error();
		}
	}

	function expectArray(value) {

		if (isArray(value)) {
			// ok
		}
		else {
			throw new Error();
		}
	}

	function evaluateArrayLiteral(literal) {

		const result = [];

		for (const item of literal) {

			result.push(
				evaluate(
					item
				)
			);
		}

		return result;
	}

	function evaluateObjectLiteral(literal) {

		const result = {};

		for (const name in literal) {

			const value = evaluate(
				literal[name]
			);

			if (value === undefined) {
				// ok
			}
			else {
				result[name] = value;
			}
		}

		return result;
	}

	function evaluateUndefinedExpression(content) {

		const value = evaluate(
			content
		);

		expectUndefined(
			value
		);

		return value;
	}

	function evaluateBooleanExpression(content) {

		const value = evaluate(
			content
		);

		expectBoolean(
			value
		);

		return value;
	}

	function evaluateNumberExpression(content) {

		const value = evaluate(
			content
		);

		expectNumber(
			value
		);

		return value;
	}

	function evaluateStringExpression(content) {

		const value = evaluate(
			content
		);

		expectString(
			value
		);

		return value;
	}

	function evaluateArrayExpression(content) {

		const value = evaluate(
			content
		);

		expectArray(
			value
		);

		return value;
	}

	function evaluateObjectExpression(content) {

		if (content === null) {
			return null;
		}

		if (typeof content === "object") {
			// ok
		}
		else {
			throw new Error();
		}

		const result = {};

		if (isArray(content)) {

			for (const item of content) {

				const name = evaluate(
					item.name
				);

				expectString(
					name
				);

				const value = evaluate(
					item.value
				);

				if (value === undefined) {
					// ok
				}
				else {
					result[name] = value;
				}
			}
		}
		else {

			for (const name in content) {

				const value = evaluate(
					content[name]
				);

				if (value === undefined) {
					// ok
				}
				else {
					result[name] = value;
				}
			}
		}

		return result;
	}

	function evaluateLookupExpression(content) {

		const name = evaluate(
			content
		);

		expectString(
			name
		);

		return context[name];
	}

	function evaulateAddExpression(content) {

		const left = evaluate(
			content.left
		);

		expectNumber(
			left
		);

		const right = evaluate(
			content.right
		);

		expectNumber(
			right
		);

		return left + right;
	}

	function evaluateConcatExpression(content) {

		if (isArray(content)) {

			const list = [];
			for (const item of content) {

				const value = evaluate(
					item
				);

				expectString(
					value
				);

				list.push(
					value
				);
			}

			return list.join("");
		}
		else {

			const left = evaluate(
				content.left
			);

			expectString(
				left
			);

			const right = evaluate(
				content.right
			);

			expectString(
				right
			);

			return left + right;
		}
	}

	return evaluate(
		expression
	);
}

module.exports = {
	transform
};
