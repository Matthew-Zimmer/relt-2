import { RefreshInstruction } from "../../../relt/analysis/validInstructions";
import { ScalaObjectDefinition } from "../../types";
import { makeInstructionClass, withDeltaColumn } from "./common";

export function convertRefreshInstruction(ins: RefreshInstruction, count: number, idx: number): ScalaObjectDefinition {
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
                name: "read"
              },
              args: [{
                kind: "ScalaIdentifierExpression",
                name: "spark"
              }]
            }
          },
          right: withDeltaColumn("created")
        },
        right: {
          kind: "ScalaIdentifierExpression",
          name: "as",
          types: [{
            kind: "ScalaDotType",
            left: {
              kind: "ScalaIdentifierType",
              name: "DeltaTypes"
            },
            right: {
              kind: "ScalaIdentifierType",
              name: ins.deltaClassName,
            }
          }]
        }
      }
    }
  ]);
}
