import { spawnSync } from "child_process";
import { mkdtemp, rm, writeFile } from "fs/promises";
import { match, P } from "ts-pattern";
import { compile } from "../../compiler";
import { deps } from "../../compiler/relt/analysis/vertex";
import { tc } from "../../compiler/relt/typechecker";
import { ReltModelDefinition, ReltType } from "../../compiler/relt/types";
import { throws } from "../../errors";
import { projectConfig } from "../../project";

export interface VisualizeArgs {
  open: boolean;
  emit: boolean;
}

export async function visualize(args: VisualizeArgs) {
  const open = args.open;
  const emit = args.emit || open;

  const project = await projectConfig();
  const { module } = await compile(project, { emit: false });

  const models = module.definitions;

  const d2File = makeD2File(models);

  await writeFile("out/vis.d2", d2File);

  try {
    if (emit) {
      const d2 = spawnSync("d2", ["-l", "elk", "out/vis.d2", "out/vis.svg"]);
      if (d2.status !== 0)
        throws(d2.stderr.toString());
      if (open) {
        const py = spawnSync("python3", ["-m", "webbrowser", "-t", `file://${process.cwd()}/out/vis.svg`])
        if (py.status !== 0)
          throws(py.stderr.toString());
      }
    }
  }
  finally {
    await rm("out/vis.d2");
  }
}

function makeD2File(models: ReltModelDefinition[]): string {
  return `\
direction: right

${models.map(d2sqlBlock).join('\n')}
${d2Connections(models)}
`;
}

function d2Connections(models: ReltModelDefinition[]): string {
  return models.flatMap(x => deps.feedsInto(x.name).map(y => `${x.name} -> ${y}`)).join('\n');
}

function d2sqlBlock(model: ReltModelDefinition): string {
  const type = tc.typeCheck(model);
  return `\
${model.name}: {
  shape: sql_table
${type.properties.map(x => `  ${x.name}: ${d2Type(x.type)}`).join('\n')}
}
`;
}

function d2Type(x: ReltType): string {
  return match(x)
    .with({ kind: "ReltArrayType" }, x => `${d2Type(x.of)}[]`)
    .with({ kind: "ReltBooleanType" }, x => `bool`)
    .with({ kind: "ReltDateType" }, x => `date${x.fmt === undefined ? "" : ` "${x.fmt}"`}`)
    .with({ kind: "ReltFloatType" }, x => `float`)
    .with({ kind: "ReltIdentifierType" }, x => x.name)
    .with({ kind: "ReltIntegerType" }, x => `int`)
    .with({ kind: "ReltJsonType" }, x => `${d2Type(x.of)}`)
    .with({ kind: "ReltOptionalType" }, x => `${d2Type(x.of)}?`)
    .with({ kind: "ReltStringType" }, x => `string`)
    .with({ kind: "ReltStructType" }, x => throws(`Cannot convert a struct type to a d2 type`))
    .exhaustive()
}
