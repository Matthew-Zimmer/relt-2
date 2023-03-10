import { Project } from "../../../project";
import { getValue, isExternalModel } from "../../relt/analysis/validInstructions";
import { ReltExternalModelModifier, ReltModelDefinition } from "../../relt/types";
import { formatScala } from "../format";
import { makeDeltaTypes } from "./deltaTypes";
import { makeProjectEntrypoint } from "./entrypoint";
import { makeInstructions } from "./instructions";
import { makeSparkHeader, makeSparkPrefix } from "./prefix";
import { makeStorages } from "./storages";
import { makeTypes } from "./types";

export function makeSparkEntryPoint(project: Project, models: ReltModelDefinition[]): string {
  return (
    makeSparkHeader(project) +
    makeExternalImports(models) + "\n" +
    makeSparkPrefix(project) + "\n" +
    [
      makeTypes(models),
      makeDeltaTypes(models),
      makeStorages(models),
      makeInstructions(models),
      makeProjectEntrypoint(project, models),
    ]
      .map(formatScala)
      .join('\n\n')
  );
}

export function makeExternalImports(models: ReltModelDefinition[]): string {
  const externalModels = models.filter(isExternalModel);

  return externalModels.map(x => `import ${getValue((x.modifiers.find(x => x.kind === "ReltExternalModelModifier")! as ReltExternalModelModifier).value)}\n`).join('');
}