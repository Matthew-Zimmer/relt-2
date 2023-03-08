import { DeriveInstruction } from "../../../relt/analysis/validInstructions";
import { ScalaObjectDefinition } from "../../types";
import { makeInstructionClass } from "./common";

export function convertDeriveInstruction(ins: DeriveInstruction, count: number, idx: number): ScalaObjectDefinition {
  return makeInstructionClass(ins.className, count, idx, [
    // TODO
  ]);
}
