import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";

export function makeStoreInstructionFor(model: ReltModelDefinition): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: `Store${model.name}Instruction`,
    extends: "Instruction",
    properties: [],
  };
}
