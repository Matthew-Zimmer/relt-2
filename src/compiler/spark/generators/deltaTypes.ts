import { ReltModelDefinition } from "../../relt/types";
import { ScalaObjectDefinition } from "../types";

export function makeDeltaTypes(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "DeltaTypes",
    properties: [],
  };
}
