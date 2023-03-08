import { StoreInstruction } from "../../../relt/analysis/validInstructions";
import { ScalaObjectDefinition } from "../../types";
import { makeInstructionClass } from "./common";

export function convertStoreInstruction(ins: StoreInstruction, count: number, idx: number): ScalaObjectDefinition {
  return makeInstructionClass(ins.className, count, idx, [
    {
      kind: "ScalaValExpression",
      name: "ds",
      value: {
        kind: "ScalaDotExpression",
        hints: { indent: true },
        left: {
          kind: "ScalaDotExpression",
          hints: { indent: true },
          left: {
            kind: "ScalaIdentifierExpression",
            name: "Storages"
          },
          right: {
            kind: "ScalaIdentifierExpression",
            name: ins.storageClassName,
          }
        },
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "write"
          },
          args: [{
            kind: "ScalaIdentifierExpression",
            name: "spark"
          }, {
            kind: "ScalaDotExpression",
            left: {
              kind: "ScalaIdentifierExpression",
              name: "dss"
            },
            right: {
              kind: "ScalaIdentifierExpression",
              name: `_${idx}`
            }
          }]
        }
      }
    }
  ], { noop: true });
}
