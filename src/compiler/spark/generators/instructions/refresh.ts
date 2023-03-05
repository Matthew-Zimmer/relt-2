import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";

export function makeRefreshInstructionFor(model: ReltModelDefinition): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: `Refresh${model.name}Instruction`,
    extends: "Instruction",
    properties: [],
  };
}
