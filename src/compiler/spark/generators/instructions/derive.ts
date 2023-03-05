import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";

export function makeDeriveInstructionFor(model: ReltModelDefinition): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: `Derive${model.name}Instruction`,
    extends: "Instruction",
    properties: [],
  };
}
