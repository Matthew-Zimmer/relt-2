import { Project } from "../../../project";
import { ReltModelDefinition } from "../../relt/types";
import { formatScala } from "../format";
import { makeDeltaTypes } from "./deltaTypes";
import { makeProjectEntrypoint } from "./entrypoint";
import { makeInstructions } from "./instructions";
import { makeSparkPrefix } from "./prefix";
import { makeStorages } from "./storages";
import { makeTypes } from "./types";

export function makeSparkEntryPoint(project: Project, models: ReltModelDefinition[]): string {
  return makeSparkPrefix() + "\n" + [
    makeTypes(models),
    makeDeltaTypes(models),
    makeStorages(models),
    makeInstructions(models),
    makeProjectEntrypoint(project, models),
  ].map(formatScala).join('\n\n');
}
