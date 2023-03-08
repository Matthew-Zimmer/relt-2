import { datasetsType, ScalaExpression, ScalaObjectDefinition } from "../../types";

export function makeInstructionClass(name: string, count: number, idx: number, expressions: ScalaExpression[], options?: { noop?: boolean }): ScalaObjectDefinition {
  const noop = options?.noop ?? false;

  return {
    kind: "ScalaObjectDefinition",
    name,
    extends: "Instruction",
    properties: [{
      kind: "ScalaDefExpression",
      name: "execute",
      args: [{
        name: "spark",
        type: {
          kind: "ScalaIdentifierType",
          name: "SparkSession",
        }
      }, {
        name: "dss",
        type: datasetsType(),
      }],
      returnType: datasetsType(),
      body: {
        kind: "ScalaGroupExpression",
        expressions: [
          {
            kind: "ScalaImportExpression",
            value: {
              kind: "ScalaDotExpression",
              left: {
                kind: "ScalaDotExpression",
                left: {
                  kind: "ScalaIdentifierExpression",
                  name: "spark"
                },
                right: {
                  kind: "ScalaIdentifierExpression",
                  name: "implicits"
                }
              },
              right: {
                kind: "ScalaIdentifierExpression",
                name: "_"
              }
            }
          },
          ...expressions,
          {
            kind: "ScalaReturnExpression",
            value: (noop ?
              {
                kind: "ScalaIdentifierExpression",
                name: "dss"
              } :
              {
                kind: "ScalaTupleExpression",
                values: Array.from({ length: count }).map((_, i) => i + 1 === idx ?
                  {
                    kind: "ScalaIdentifierExpression",
                    name: "ds"
                  } :
                  {
                    kind: "ScalaDotExpression",
                    left: {
                      kind: "ScalaIdentifierExpression",
                      name: "dss"
                    },
                    right: {
                      kind: "ScalaIdentifierExpression",
                      name: `_${i + 1}`
                    }
                  }
                )
              }
            ),
          }
        ],
      },
    }],
  };
}

export function withDeltaColumn(kind: "created" | "neutral" | "updated" | "deleted"): ScalaExpression {
  return {
    kind: "ScalaAppExpression",
    func: {
      kind: "ScalaIdentifierExpression",
      name: "withColumn"
    },
    args: [{
      kind: "ScalaDotExpression",
      left: {
        kind: "ScalaIdentifierExpression",
        name: "DeltaState"
      },
      right: {
        kind: "ScalaIdentifierExpression",
        name: "column"
      }
    }, {
      kind: "ScalaAppExpression",
      func: {
        kind: "ScalaIdentifierExpression",
        name: "lit"
      },
      args: [{
        kind: "ScalaDotExpression",
        left: {
          kind: "ScalaIdentifierExpression",
          name: "DeltaState"
        },
        right: {
          kind: "ScalaIdentifierExpression",
          name: kind,
        }
      }]
    }]
  }
}
