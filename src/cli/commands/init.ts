import { spawnSync } from "child_process";
import { mkdir, writeFile } from "fs/promises";
import { match } from "ts-pattern";
import { throws } from "../../errors";

export interface InitArgs {
  name: string;
  adapters: string[];
}

export async function init(args: InitArgs) {
  const { name, adapters } = args;

  const config = {
    name: name,
    ...adapters.map(x => {
      return match(x)
        .with('aws', () => ({ aws: { region: "<region>", table: "<dynamo-table-name>", bucket: "<s3-bucket-name>" } }))
        .with('databricks', () => ({ databricks: { host: "<account>.cloud.databricks.com" } }))
        .with('webex', () => ({ webex: { roomId: "<id>" } }))
        .otherwise((x) => throws(`Unknown using: ${x}`));
    }),
  };

  await writeFile("reltconfig.json", JSON.stringify(config, undefined, 2));

  await mkdir("src");

  await writeFile("src/main.relt", "");

  await match(cicd)
    .with(undefined, async () => { })
    .with("gitlab-previews", async () => {
      return Promise.allSettled(
        gitlabPreviewsScripts()
          .map(([path, cnt]) => writeFile(path, cnt))
      );
    })
    .otherwise(async () => {
      throw new Error();
    });

  await writeFile(".gitignore",
    `\
target/
`
  );

  spawnSync("git", ["init"]);
  spawnSync("git", ["add", "."]);
  spawnSync("git", ["commit", "-m", "Initial Commit"]);
}
