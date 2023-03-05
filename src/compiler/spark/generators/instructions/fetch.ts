import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";

export function makeFetchInstructionFor(model: ReltModelDefinition): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: `Fetch${model.name}Instruction`,
    extends: "Instruction",
    properties: [],
  };
}
