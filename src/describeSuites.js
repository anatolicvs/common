"use strict";
// fix
const { tools, NoLog } = require("@fiyuu/common");

class MockTools {

	begin() {

		const m31 = Math.pow(2, 31) - 1;

		function getId(value, name) {

			if (tools.isCode(value)) {
				return value;
			}

			if (name) {
				throw new Error(
					tools.format(
						"%s (%j) is not an id.",
						name,
						value
					)
				);
			}

			throw new Error(
				tools.format(
					"(%j) is not an id.",
					value
				)
			);
		}

		function ogetId(value, name) {

			if (value === undefined) {
				return;
			}

			return getId(value, name);
		}

		const rng32hex = () => {

			const id = this.idseed++;
			return `id-${this.idseed++}`;
		}

		const originals = {
			newid: tools.newid,
			isId: tools.isId,
			asId: tools.asId,
			getId: tools.getId,
			ogetId: tools.ogetId,
			uuid: tools.patterns.uuid,
			ts: tools.ts,
			random: Math.random,
			rng32hex: tools.rng32hex
		};

		this.originals = originals;

		tools.newid = () => {
			return `id-${this.idseed++}`;
		};

		tools.isId = tools.isCode;
		tools.asId = function (value) {
			if (tools.isCode(value)) {
				return value;
			}
		};

		tools.getId = getId;
		tools.ogetId = ogetId;

		tools.patterns.uuid = tools.patterns.code;

		tools.ts = () => {
			return this.ts;
		}

		Math.random = () => {
			const r = this.rngseed * 48271 % m31;
			this.rngseed = r;
			return (r - 1) / (m31 - 1);
		};

		tools.rng32hex = rng32hex;
	}

	end() {

		const originals = this.originals;
		this.originals = null;

		Math.random = originals.random;

		tools.newid = originals.newid;
		tools.rng32hex = originals.rng32hex;
		tools.isId = originals.isId;
		tools.asId = originals.asId;
		tools.getId = originals.getId;
		tools.ogetId = originals.ogetId;
		tools.patterns.uuid = originals.uuid;
		tools.ts = originals.ts;
	}
}

MockTools.prototype.originals = null;
MockTools.prototype.idseed = 0;
MockTools.prototype.rngseed = 1;
MockTools.prototype.ts = 0;

class MockDataAccess {

	constructor() {
		this.queue = [];
	}

	da(method, args) {

		if (0 < this.queue.length) {
			// ok
		}
		else {
			throw new Error(
				tools.format(
					"unexpected request %s %j",
					method,
					args
				)
			);
		}

		const item = this.queue.shift();

		if (item.method === method) {
			// ok
		}
		else {
			throw new Error();
		}

		if (JSON.stringify(args) === JSON.stringify(item.request)) {
			// ok
		}
		else {

			console.log(args);
			console.log(item.request);
			throw new Error();
		}

		return Promise.resolve(
			item.response
		);

	}

	getCachedVersioned(...args) {

		return this.da("get-cached-versioned", args);
	}

	getConsistent(...args) {

		return this.da("get-consistent", args);
	}

	expect(method, request, response) {

		this.queue.push({
			method,
			request,
			response
		});
	}

	assert() {

		if (0 < this.queue.length) {
			throw new Error();
		}
	}

	async begin() {

	}

	async end() {

		if (0 < this.queue.length) {
			throw new Error();
		}
	}
}

MockDataAccess.prototype.queue = null;

class MockSQSClient {

	constructor() {
		this.queue = [];
	}

	sendMessage(request) {

		if (0 < this.queue.length) {
			// ok
		}
		else {
			throw new Error();
		}

		const item = this.queue.shift();

		if (item.method === "send-message") {
			// ok
		}
		else {
			throw new Error();
		}

		if (JSON.stringify(request) === JSON.stringify(item.request)) {
			// ok
		}
		else {

			console.log(request);
			console.log(item.request);
			throw new Error();
		}

		return {
			promise() {
				return item.response;
			}
		};
	}

	expect(method, request, response) {

		this.queue.push({
			method,
			request,
			response
		});
	}

	assert() {

		if (0 < this.queue.length) {
			throw new Error();
		}
	}

	async begin() {

	}

	async end() {

		if (0 < this.queue.length) {
			throw new Error();
		}
	}
}

MockSQSClient.prototype.queue = null;

function describeSuites(createInstances, ...suites) {

	describe("level 1", () => {

		for (const suite of suites) {

			describe(suite.desc, () => {

				executeSuite(createInstances, suite);
			});
		}
	});

}

function executeSuite(createInstances, suite) {

	const mockTools = new MockTools();
	const mockSQSClient = new MockSQSClient();
	const mockDataAccess = new MockDataAccess();

	const mock = {
		mockTools,
		mockSQSClient,
		mockDataAccess
	};

	// before(async () => {

	// });

	// after(async () => {

	// });

	beforeEach(async () => {

		await mockTools.begin();

		mockTools.idseed = 0;
		mockTools.rngseed = 1;
		mockTools.ts = 0;

		await mockDataAccess.begin();
		await mockSQSClient.begin();
	});

	afterEach(async () => {

		await mockSQSClient.end();
		await mockDataAccess.end();
		await mockTools.end();
	});


	for (const scenario of suite.scenarios) {

		it(scenario.description, async function () {

			if (scenario.slow) {
				this.slow(
					scenario.slow
				);
			}

			if (scenario.timeout) {
				this.timeout(
					scenario.timeout
				);
			}

			await executeScenario(
				mock,
				createInstances,
				suite,
				scenario
			);
		});
	}
}

async function executeScenario(mock, createInstances, suite, scenario) {

	const {
		mockTools,
		mockSQSClient,
		mockDataAccess
	} = mock;

	const instances = createInstances({
		createLog() {
			return NoLog.instance;
		},
		da: mockDataAccess,
		sqs: mockSQSClient
	});

	let scenarioOverrideInfos;
	if (scenario.overrides) {

		for (const instanceName in scenario.overrides) {

			const instanceOverride = scenario.overrides[instanceName];
			const instance = instances[instanceName];
			if (instance === undefined) {
				throw new Error(instanceName);
			}

			for (const methodName in instanceOverride) {

				const methodOverride = instanceOverride[methodName];
				const method = instance[methodName];
				if (method === undefined) {
					throw new Error();
				}

				// console.log("override %j %j", instanceName, methodName);
				instance[methodName] = methodOverride;

				if (scenarioOverrideInfos === undefined) {
					scenarioOverrideInfos = [];
				}

				scenarioOverrideInfos.push({
					instance,
					methodName,
					method
				});
			}
		}
	}

	try {

		if (Number.isInteger(scenario.setTime)) {
			mockTools.ts = scenario.setTime;
		}

		let prepare = scenario.prepare;
		if (prepare !== undefined) {

		}

		//let client = new Client();
		if (scenario.steps) {

			for (const step of scenario.steps) {

				if (step.type === "set-time") {

					mockTools.ts = step.value;
				}
				else if (step.type === "inc-time") {

					mockTools.ts += step.value;
				}
				else if (step.type === "da-expect") {

					mockDataAccess.expect(
						step.method,
						step.request,
						step.response
					);
				}
				else if (step.type === "sqs-expect") {

					mockSQSClient.expect(
						step.method,
						step.request,
						step.response
					);
				}
				else if (step.type === "sqs-assert") {

					mockSQSClient.assert();
				}
				else {

					let overrideInfos;
					if (step.overrides) {

						for (const instanceName in step.overrides) {

							const serviceOverride = steps.overrides[instanceName];
							const instance = instances[instanceName];
							if (instance === undefined) {
								throw new Error();
							}

							for (const methodName in serviceOverride) {

								const methodOverride = serviceOverride[methodName];
								const method = instance[methodName];
								if (method === undefined) {
									throw new Error();
								}

								instance[methodName] = methodOverride;

								if (overrideInfos === undefined) {
									overrideInfos = [];
								}

								overrideInfos.push({
									instance,
									methodName,
									method
								});
							}
						}
					}

					try {

						let instance;
						if (step.instance) {
							instance = instances[step.instance];
						}
						else if (scenario.instance) {
							instance = instances[scenario.instance];
						}
						else if (suite.instance) {
							instance = instances[suite.instance];
						}
						// else {
						// 	instance = client;
						// }

						let method;
						if (step.method) {
							method = step.method;
						}
						else if (scenario.method) {
							method = scenario.method;
						}
						else if (suite.method) {
							method = suite.method;
						}
						else {
							throw new Error();
						}

						await invokeMethod(
							instance,
							method,
							step
						);
					}
					finally {

						if (overrideInfos !== undefined) {
							for (const overrideInfo of overrideInfos) {
								overrideInfo.instance[overrideInfo.methodName] = overrideInfo.method;
							}
						}
					}
				}
			}

		}
		else {

			let instance;
			if (scenario.instance) {
				instance = instances[scenario.instance];
			}
			else if (suite.instance) {
				instance = instances[suite.instance];
			}
			// else {
			// 	instance = client;
			// }

			let method;
			if (scenario.method) {
				method = scenario.method;
			}
			else if (suite.method) {
				method = suite.method;
			}
			else {
				throw new Error();
			}

			await invokeMethod(
				instance,
				method,
				scenario
			);
		}
	}
	finally {

		if (scenarioOverrideInfos !== undefined) {
			for (const overrideInfo of scenarioOverrideInfos) {
				overrideInfo.instance[overrideInfo.methodName] = overrideInfo.method;
			}
		}
	}
}

async function invokeMethod(instance, method, test) {

	if (test.throws) {

		let response;
		try {

			if (test.args !== undefined) {

				response = await instance[method](
					...test.args
				);
			}
			else if (test.request !== undefined) {

				response = await instance[method](
					test.request
				);
			}
			else {

				response = await instance[method]();
			}

		}
		catch (error) {

			if (typeof test.throws === "string") {
				if (error.message === test.throws) {
					return;
				}
			}
			else {

				for (const key in test.throws) {

					const actual = JSON.stringify(error[key]);
					const expected = JSON.stringify(test.throws[key]);
					if (actual === expected) {
						// ok
					}
					else {
						throw new Error(`${actual}\n!=\n${expected}\n`);
					}
				}
				return;
			}

			throw error;
		}

		throw new Error(
			tools.format(
				"no throw (%j). response: %j",
				test.throws,
				response
			)
		);
	}
	else {

		let response;

		if (test.args !== undefined) {

			response = await instance[method](
				...test.args
			);
		}
		else if (test.request !== undefined) {

			response = await instance[method](
				test.request
			);
		}
		else {
			response = await instance[method]();
		}

		if (test.response === undefined) {

			if (test.responseSchema === undefined) {

				if (response === undefined) {
					// ok
				}
				else {

					throw new Error(
						tools.format(
							"unexpected response: %j",
							response
						)
					);
				}
			}
			else {

				throw new Error();
				// if (response === undefined) {
				// 	throw new Error();
				// }

				// const validator = new Validator();
				// const validationResult = validator.validate(response, test.responseSchema);
				// if (0 < validationResult.errors.length) {

				// 	for (const error of validationResult.errors) {

				// 		console.log(
				// 			error
				// 		);
				// 	}

				// 	throw new Error();
				// }
			}
		}
		else {

			if (response === undefined) {

				throw new Error("no response.");
			}
			else {

				const responseJson = JSON.stringify(response);
				const testResponseJson = JSON.stringify(test.response);

				if (testResponseJson === responseJson) {
					// ok
				}
				else {
					throw new Error(`${method}\n${responseJson}\n!=\n${testResponseJson}\n`);
				}
			}
		}
	}
}

module.exports = {
	describeSuites
};
