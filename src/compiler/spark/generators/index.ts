import { Project } from "../../../project";
import { ReltModelDefinition } from "../../relt/types";
import { formatScala } from "../format";
import { ScalaObjectDefinition } from "../types";
import { makeDeltaTypes } from "./deltaTypes";
import { makeProjectEntrypoint } from "./entrypoint";
import { makeInstructions } from "./instructions";
import { makeStorages } from "./storages";
import { makeTypes } from "./types";

export function makeProject(project: Project, models: ReltModelDefinition[]): ScalaObjectDefinition[] {
  return [
    makeTypes(models),
    makeDeltaTypes(models),
    makeStorages(models),
    makeInstructions(models),
    makeProjectEntrypoint(project, models),
  ];
}

export function toScala(project: Project, models: ReltModelDefinition[]): string {
  return makeProject(project, models).map(formatScala).join('\n');
}
