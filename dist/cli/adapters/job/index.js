"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.jobAdapters = void 0;
const databricks_1 = require("./databricks");
exports.jobAdapters = {
    databricks: databricks_1.databricksJobAdapter,
};
