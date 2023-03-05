"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.runnerAdapters = void 0;
const gitlab_1 = require("./gitlab");
exports.runnerAdapters = {
    gitlab: gitlab_1.gitlabRunnerAdapter,
};
