"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.awsCloudAdapter = void 0;
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const client_s3_1 = require("@aws-sdk/client-s3");
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const promises_1 = require("fs/promises");
const errors_1 = require("../../../errors");
function awsCloudAdapter(project) {
    const { aws: { region, table, bucket } } = project;
    const ddb = new client_dynamodb_1.DynamoDBClient({
        region,
    });
    const s3 = new client_s3_1.S3Client({
        region,
    });
    return {
        // dynamo
        delete: (key) => __awaiter(this, void 0, void 0, function* () {
            yield ddb.send(new client_dynamodb_1.DeleteItemCommand({
                Key: (0, util_dynamodb_1.marshall)({ key }),
                TableName: table,
            }));
        }),
        load: (key) => __awaiter(this, void 0, void 0, function* () {
            return yield ddb.send(new client_dynamodb_1.GetItemCommand({
                Key: (0, util_dynamodb_1.marshall)({ key }),
                TableName: table,
            })).then(x => x.Item === undefined ? (0, errors_1.throws)(`${key} not found`) : (0, util_dynamodb_1.unmarshall)(x.Item));
        }),
        save: (key, value) => __awaiter(this, void 0, void 0, function* () {
            yield ddb.send(new client_dynamodb_1.PutItemCommand({
                Item: (0, util_dynamodb_1.marshall)(Object.assign({ key }, value)),
                TableName: table,
            }));
        }),
        // s3
        remove: (path) => __awaiter(this, void 0, void 0, function* () {
            yield s3.send(new client_s3_1.DeleteObjectCommand({
                Bucket: bucket,
                Key: path,
            }));
        }),
        upload: (from, to) => __awaiter(this, void 0, void 0, function* () {
            yield s3.send(new client_s3_1.PutObjectCommand({
                Bucket: bucket,
                Key: to,
                Body: yield (0, promises_1.readFile)(from)
            }));
        }),
    };
}
exports.awsCloudAdapter = awsCloudAdapter;
