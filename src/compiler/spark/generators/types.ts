import { ReltModelDefinition } from "../../relt/types";
import { ScalaObjectDefinition } from "../types";

export function makeTypes(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "Types",
    properties: [],
  };
}
