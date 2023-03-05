"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.adapters = void 0;
const errors_1 = require("../../errors");
const alert_1 = require("./alert");
const cloud_1 = require("./cloud");
const job_1 = require("./job");
const runner_1 = require("./runner");
function adapters(project, required) {
    const keys = new Set(Object.keys(project));
    const createAdapter = (kind, adapters) => {
        const results = Object.keys(adapters).filter(x => keys.has(x));
        if (results.length === 0) {
            if (required && required[kind] === true)
                (0, errors_1.throws)(`A ${kind} adapter is required for this operation: ${required.op}`);
            return undefined;
        }
        if (results.length !== 1)
            throw new Error(`project config has multiple ${kind} adapters: ${results.join()}`);
        return adapters[results[0]](project);
    };
    const alert = createAdapter('alert', alert_1.alertAdapters);
    const cloud = createAdapter('cloud', cloud_1.cloudAdapters);
    const job = createAdapter('job', job_1.jobAdapters);
    const runner = createAdapter('runner', runner_1.runnerAdapters);
    return {
        alert,
        cloud,
        job,
        runner,
    };
}
exports.adapters = adapters;
