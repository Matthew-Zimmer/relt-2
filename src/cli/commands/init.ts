import { spawnSync } from "child_process";
import { mkdir, writeFile } from "fs/promises";
import { match } from "ts-pattern";
import { throws } from "../../errors";
import { Project } from "../../project";
import { adapters } from "../adapters";

export interface InitArgs {
  name: string;
  adapters: string[];
}

export async function init(args: InitArgs) {
  const { name, adapters: requestedAdapters } = args;

  await mkdir(name);

  const project = {
    name: name,
    ...requestedAdapters.reduce((p, c) => {
      return {
        ...p,
        ...match(c)
          .with('aws', () => ({ aws: { region: "<region>", table: "<dynamo-table-name>", bucket: "<s3-bucket-name>" } }))
          .with('databricks', () => ({ databricks: { host: "https://<account>.cloud.databricks.com" } }))
          .with('webex', () => ({ webex: { roomId: "<id>", host: "https://<webex-url>" } }))
          .with('gitlab', () => ({ gitlab: {} }))
          .otherwise((x) => throws(`Unknown adapter: ${x}`))
      };
    }, {}),
  } satisfies Project;

  await writeFile(`${name}/reltconfig.json`, JSON.stringify(project, undefined, 2));

  await mkdir(`${name}/src`);

  await writeFile(`${name}/src/main.relt`, "");

  const { runner } = adapters(project);

  await runner?.createConfigs();

  await writeFile(".gitignore",
    `\
target/
`
  );

  spawnSync("git", ["init"]);
  spawnSync("git", ["add", "."]);
  spawnSync("git", ["commit", "-m", "Initial Commit"]);
}
