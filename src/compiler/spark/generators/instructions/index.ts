import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";

export function makeInstructions(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "Instructions",
    properties: [],
  };
}

export function makeInstructionsFor(model: ReltModelDefinition): ScalaObjectDefinition[] {
  return [];
}
