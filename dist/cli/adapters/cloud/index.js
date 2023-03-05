"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.cloudAdapters = void 0;
const aws_1 = require("./aws");
exports.cloudAdapters = {
    aws: aws_1.awsCloudAdapter,
};
