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
exports.destroy = void 0;
const project_1 = require("../../project");
const adapters_1 = require("../adapters");
function destroy(args) {
    return __awaiter(this, void 0, void 0, function* () {
        const { branch } = args;
        const project = yield (0, project_1.projectConfig)();
        const { job, alert, cloud } = (0, adapters_1.adapters)(project, {
            op: "destroy",
            job: true,
            cloud: true,
        });
        yield cloud.remove("");
        const state = yield cloud.load(branch);
        yield job.remove(state.job.id);
        yield cloud.delete(branch);
        yield (alert === null || alert === void 0 ? void 0 : alert.msg(`Removed job for ${project.name}:${branch}`));
    });
}
exports.destroy = destroy;
