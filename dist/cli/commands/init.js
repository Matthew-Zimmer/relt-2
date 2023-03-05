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
exports.init = void 0;
const child_process_1 = require("child_process");
const promises_1 = require("fs/promises");
const ts_pattern_1 = require("ts-pattern");
const errors_1 = require("../../errors");
const adapters_1 = require("../adapters");
function init(args) {
    return __awaiter(this, void 0, void 0, function* () {
        const { name, adapters: requestedAdapters } = args;
        yield (0, promises_1.mkdir)(name);
        const project = Object.assign({ name: name }, requestedAdapters.reduce((p, c) => {
            return Object.assign(Object.assign({}, p), (0, ts_pattern_1.match)(c)
                .with('aws', () => ({ aws: { region: "<region>", table: "<dynamo-table-name>", bucket: "<s3-bucket-name>" } }))
                .with('databricks', () => ({ databricks: { host: "https://<account>.cloud.databricks.com" } }))
                .with('webex', () => ({ webex: { roomId: "<id>", host: "https://<webex-url>" } }))
                .with('gitlab', () => ({ gitlab: {} }))
                .otherwise((x) => (0, errors_1.throws)(`Unknown adapter: ${x}`)));
        }, {}));
        yield (0, promises_1.writeFile)(`${name}/reltconfig.json`, JSON.stringify(project, undefined, 2));
        yield (0, promises_1.mkdir)(`${name}/src`);
        yield (0, promises_1.writeFile)(`${name}/src/main.relt`, "");
        const { runner } = (0, adapters_1.adapters)(project);
        yield (runner === null || runner === void 0 ? void 0 : runner.createConfigs());
        yield (0, promises_1.writeFile)(".gitignore", `\
target/
`);
        (0, child_process_1.spawnSync)("git", ["init"]);
        (0, child_process_1.spawnSync)("git", ["add", "."]);
        (0, child_process_1.spawnSync)("git", ["commit", "-m", "Initial Commit"]);
    });
}
exports.init = init;
