#!/usr/bin/env node
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const yargs_1 = __importDefault(require("yargs"));
const commands_1 = require("./cli/commands");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            yargs_1.default
                .scriptName("relt")
                .usage('$0 <cmd> [args]')
                .command('init', 'Create a new relt project', yargs => yargs
                .options({
                name: {
                    string: true,
                    required: true,
                },
                adapters: {
                    array: true,
                    string: true,
                    default: [],
                }
            }), args => (0, commands_1.init)(args))
                .command('compile', 'Compiles the relt project', yargs => yargs
                .options({
                "to-jar": {
                    boolean: true,
                }
            }), args => { (0, commands_1.compile)(args); })
                .command('deploy', 'Deploys the project tto an environment (uses cloud,job,[alert] adapters)', yargs => yargs
                .positional("branch", {
                type: "string",
                demandOption: true,
            })
                .options({
                using: {
                    array: true,
                    string: true,
                    default: [],
                    choices: ["webex", "databricks"],
                }
            }), args => (0, commands_1.deploy)(args))
                .command('redeploy', 'Redeploys existing project to an environment (uses cloud,job,[alert] adapters)', yargs => yargs
                .positional("branch", {
                type: "string",
                demandOption: true,
            })
                .options({
                using: {
                    array: true,
                    string: true,
                    default: [],
                    choices: ["webex", "databricks"],
                }
            }), args => (0, commands_1.redeploy)(args))
                .command('destroy', 'Destroys the production from a environment (uses cloud,job adapters)', yargs => yargs
                .positional("branch", {
                type: "string",
                demandOption: true,
            })
                .options({
                using: {
                    array: true,
                    string: true,
                    default: [],
                    choices: ["webex", "databricks"],
                }
            }), args => (0, commands_1.destroy)(args))
                .help()
                .version("0.0.0")
                .argv;
        }
        catch (e) {
            console.error(e);
        }
    });
}
main();
