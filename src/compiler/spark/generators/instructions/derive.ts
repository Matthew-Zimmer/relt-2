import { match } from "ts-pattern";
import { ColumnExpression, DeriveInstruction, DeriveStep } from "../../../relt/analysis/validInstructions";
import { ReltExpression } from "../../../relt/types";
import { ScalaExpression, ScalaObjectDefinition } from "../../types";
import { convertType } from "../types";
import { makeInstructionClass } from "./common";

export function convertDeriveInstruction(ins: DeriveInstruction, count: number, idx: number): ScalaObjectDefinition {
  return makeInstructionClass(ins.className, count, idx, ins.steps.map(convertDeriveStep));
}

export function ds(idx: number): ScalaExpression {
  return {
    kind: "ScalaIdentifierExpression",
    name: `ds${idx}`
  };
}

export function df(ds: ScalaExpression): ScalaExpression {
  return {
    kind: "ScalaDotExpression",
    left: ds,
    right: {
      kind: "ScalaIdentifierExpression",
      name: "toDF",
    }
  };
}

export function convertColumnExpression(x: ColumnExpression): ScalaExpression {
  switch (x.kind) {
    case "ColumnColExpression":
      return {
        kind: "ScalaAppExpression",
        func: {
          kind: "ScalaIdentifierExpression",
          name: "col"
        },
        args: [{
          kind: "ScalaStringExpression",
          value: x.name
        }],
      };
    case "ColumnLiteralExpression":
      return {
        kind: "ScalaAppExpression",
        func: {
          kind: "ScalaIdentifierExpression",
          name: "lit"
        },
        args: [
          match(x.value)
            .with({ kind: "ReltBooleanExpression" }, x => ({
              kind: "ScalaBooleanExpression",
              value: x.value,
            }) satisfies ScalaExpression)
            .with({ kind: "ReltFloatExpression" }, x => ({
              kind: "ScalaFloatExpression",
              value: x.value,
            }) satisfies ScalaExpression)
            .with({ kind: "ReltIntegerExpression" }, x => ({
              kind: "ScalaIntegerExpression",
              value: x.value,
            }) satisfies ScalaExpression)
            .with({ kind: "ReltStringExpression" }, x => ({
              kind: "ScalaStringExpression",
              value: x.value,
            }) satisfies ScalaExpression)
            .exhaustive()
        ],
      };
    case "ColumnBinaryOpExpression":
      return {
        kind: "ScalaBinaryOpExpression",
        left: convertColumnExpression(x.left),
        op: x.op,
        right: convertColumnExpression(x.right),
      };
    case "ColumnAppExpression":
      return {
        kind: "ScalaDotExpression",
        left: convertColumnExpression(x.col),
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: x.func,
          },
          args: []
        }
      };
    case "ColumnWhenExpression":
      return {
        kind: "ScalaDotExpression",
        left: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "when"
          },
          args: [
            convertColumnExpression(x.condition),
            convertColumnExpression(x.ifTrue),
          ]
        },
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "otherwise"
          },
          args: [convertColumnExpression(x.ifFalse)]
        }
      };
    case "ColumnGroupExpression":
      return {
        kind: "ScalaPartExpression",
        value: convertColumnExpression(x.value),
      };
    case "ColumnIdentifierExpression":
      return {
        kind: "ScalaIdentifierExpression",
        name: x.name,
      };
  }
}

export function convertDeriveStep(step: DeriveStep): ScalaExpression {
  return match(step)
    .with({ kind: "AsStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: ds(x.ds),
        right: {
          kind: "ScalaIdentifierExpression",
          name: "as",
          types: [{
            kind: "ScalaDotType",
            left: {
              kind: "ScalaIdentifierType",
              name: "DeltaTypes"
            },
            right: convertType(x.type),
          }],
        }
      }
    }) satisfies  ScalaExpression)
    .with({ kind: "JoinStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: {
          kind: "ScalaIdentifierExpression",
          name: "Ops"
        },
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "join"
          },
          args: [
            {
              kind: "ScalaIdentifierExpression",
              name: "spark"
            },
            df(ds(x.left)),
            df(ds(x.right)),
            convertColumnExpression(x.condition),
            {
              kind: "ScalaStringExpression",
              value: x.type
            }
          ]
        }
      }
    }) satisfies ScalaExpression)
    .with({ kind: "RetStep" }, x => ({
      kind: "ScalaValExpression",
      name: "ds",
      value: ds(x.ds),
    }) satisfies ScalaExpression)
    .with({ kind: "UseStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: {
          kind: "ScalaIdentifierExpression",
          name: "dss"
        },
        right: {
          kind: "ScalaIdentifierExpression",
          name: `_${x.dssIdx + 1}`
        }
      }
    }) satisfies ScalaExpression)
    .with({ kind: "WithStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: ds(x.ds),
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "withColumn"
          },
          args: [
            {
              kind: "ScalaStringExpression",
              value: x.name
            },
            convertColumnExpression(x.value)
          ]
        }
      }
    }) satisfies ScalaExpression)
    .with({ kind: "RenameStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: ds(x.ds),
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "withColumnRenamed"
          },
          args: [
            {
              kind: "ScalaStringExpression",
              value: x.name
            },
            convertColumnExpression(x.value)
          ]
        }
      }
    }) satisfies ScalaExpression)
    .with({ kind: "SortStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: ds(x.ds),
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "sort"
          },
          args: x.columns.map<ScalaExpression>(c => {
            return {
              kind: "ScalaDotExpression",
              left: convertColumnExpression(c),
              right: {
                kind: "ScalaIdentifierExpression",
                name: x.mode,
              }
            }
          })
        }
      }
    }) satisfies ScalaExpression)
    .with({ kind: "UnionStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: ds(x.left),
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "union"
          },
          args: [
            ds(x.right)
          ]
        }
      }
    }) satisfies ScalaExpression)
    .with({ kind: "WhereStep" }, x => ({
      kind: "ScalaValExpression",
      name: `ds${x.dest}`,
      value: {
        kind: "ScalaDotExpression",
        left: ds(x.ds),
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "where"
          },
          args: [
            convertColumnExpression(x.condition)
          ]
        }
      }
    }) satisfies ScalaExpression)
    .exhaustive();
}
