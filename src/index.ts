#!/usr/bin/env node
import yargs from 'yargs';
import { init, destroy, redeploy, deploy, compile } from './cli/commands';

async function main() {
  try {
    yargs
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
        }),
        args => init(args))
      .command('compile', 'Compiles the relt project', yargs => yargs
        .options({
          "to-jar": {
            boolean: true,
          }
        }),
        args => { compile(args) })
      .command('deploy', 'Runs the project', yargs => yargs
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
        }),
        args => deploy(args))
      .command('redeploy', 'Runs the project', yargs => yargs
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
        }),
        args => redeploy(args))
      .command('destroy', 'Runs the project', yargs => yargs
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
        }),
        args => destroy(args))
      .help()
      .version("0.0.0")
      .argv
  }
  catch (e) {
    console.error(e);
  }
}

main();
