import { Project } from "../../../project";
import { instructionsFor } from "../../relt/analysis/validInstructions";
import { vertexInfo } from "../../relt/analysis/vertex";
import { ReltModelDefinition } from "../../relt/types";
import { ScalaValExpression, thisExpression, ScalaObjectDefinition, ScalaBinaryOpExpression, ScalaExpression, ScalaVarExpression } from "../types";

export function makeInstructionSet(models: ReltModelDefinition[]): ScalaValExpression {
  const indices = Object.fromEntries(models.map((x, i) => [x.name, i]));
  return {
    kind: "ScalaValExpression",
    visibility: "private",
    name: "instructionSet",
    value: {
      kind: "ScalaAppExpression",
      func: {
        kind: "ScalaIdentifierExpression",
        name: "Map",
        types: [
          {
            kind: "ScalaStringType"
          },
          {
            kind: "ScalaOfType",
            type: {
              kind: "ScalaIdentifierType",
              name: "Map"
            },
            of: [
              {
                kind: "ScalaStringType"
              },
              {
                kind: "ScalaIdentifierType",
                name: "Instruction"
              }
            ]
          }
        ],
      },
      args: models.map(x => makeInstructionSetForModel(x, indices)),
      hints: {
        indent: true,
      }
    }
  };
}

export function makeInstructionSetForModel(model: ReltModelDefinition, indices: Record<string, number>): ScalaBinaryOpExpression {
  return {
    kind: "ScalaBinaryOpExpression",
    left: {
      kind: "ScalaStringExpression",
      value: model.name,
    },
    op: "->",
    right: {
      kind: "ScalaAppExpression",
      func: {
        kind: "ScalaIdentifierExpression",
        name: "Map"
      },
      args: instructionsFor(model, indices).map<ScalaExpression>(x => ({
        kind: "ScalaBinaryOpExpression",
        left: {
          kind: "ScalaStringExpression",
          value: x.name
        },
        op: "->",
        right: {
          kind: "ScalaDotExpression",
          left: {
            kind: "ScalaIdentifierExpression",
            name: "Instructions"
          },
          right: {
            kind: "ScalaIdentifierExpression",
            name: x.className,
          }
        }
      })),
      hints: {
        indent: true,
      }
    }
  }
}

export function makeVertices(models: ReltModelDefinition[]): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
    visibility: "private",
    name: "vertices",
    value: {
      kind: "ScalaAppExpression",
      func: {
        kind: "ScalaIdentifierExpression",
        name: "Map",
        types: [
          {
            kind: "ScalaStringType"
          },
          {
            kind: "ScalaIdentifierType",
            name: "Vertex"
          }
        ]
      },
      args: models.map(makeVertex),
      hints: {
        indent: true,
      }
    }
  }
}

export function makeVertex(model: ReltModelDefinition): ScalaBinaryOpExpression {
  const { hasStorage, parents, children } = vertexInfo(model);

  return {
    kind: "ScalaBinaryOpExpression",
    left: {
      kind: "ScalaStringExpression",
      value: model.name,
    },
    op: "->",
    right: {
      kind: "ScalaNewExpression",
      value: {
        kind: "ScalaAppExpression",
        func: {
          kind: "ScalaIdentifierExpression",
          name: "Vertex"
        },
        args: [{
          kind: "ScalaStringExpression",
          value: model.name,
        }, {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "Seq"
          },
          args: parents.map<ScalaExpression>(value => ({
            kind: "ScalaStringExpression",
            value,
          }))
        }, {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "Seq"
          },
          args: children.map<ScalaExpression>(value => ({
            kind: "ScalaStringExpression",
            value,
          }))
        }, {
          kind: "ScalaBooleanExpression",
          value: hasStorage
        }]
      }
    }
  }
}

export function makeDag(): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
    visibility: "private",
    name: "dag",
    value: {
      kind: "ScalaNewExpression",
      value: {
        kind: "ScalaAppExpression",
        func: {
          kind: "ScalaIdentifierExpression",
          name: "Dag",
        },
        args: [
          thisExpression({ kind: "ScalaIdentifierExpression", name: "vertices" }),
          thisExpression({ kind: "ScalaIdentifierExpression", name: "instructionSet" }),
        ],
        hints: {
          indent: true,
        }
      }
    }
  };
}

export function makeInterpreter(): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
    visibility: "private",
    name: "interpreter",
    value: {
      kind: "ScalaNewExpression",
      value: {
        kind: "ScalaAppExpression",
        func: {
          kind: "ScalaIdentifierExpression",
          name: "Interpreter",
        },
        args: [
          thisExpression({ kind: "ScalaIdentifierExpression", name: "dag" }),
          thisExpression({ kind: "ScalaIdentifierExpression", name: "instructionSet" }),
        ],
        hints: {
          indent: true,
        }
      }
    }
  };
}

export function makeInitialDss(models: ReltModelDefinition[]): ScalaVarExpression {
  return {
    kind: "ScalaVarExpression",
    name: "dss",
    value: {
      kind: "ScalaTupleExpression",
      hints: {
        indent: true,
      },
      values: models.map<ScalaExpression>(x => ({
        kind: "ScalaDotExpression",
        left: {
          kind: "ScalaIdentifierExpression",
          name: "spark"
        },
        right: {
          kind: "ScalaIdentifierExpression",
          name: "emptyDataset",
          types: [{
            kind: "ScalaDotType",
            left: {
              kind: "ScalaIdentifierType",
              name: "DeltaTypes"
            },
            right: {
              kind: "ScalaIdentifierType",
              name: x.name
            }
          }]
        }
      })),
    }
  }
}

export function makeProjectEntrypoint(project: Project, models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: `${project.name}`,
    properties: [
      makeInstructionSet(models),
      makeVertices(models),
      makeDag(),
      makeInterpreter(),
      {
        kind: "ScalaDefExpression",
        name: "main",
        args: [{ name: "args", type: { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Array" }, of: [{ kind: "ScalaStringType" }] } }],
        returnType: { kind: "ScalaIdentifierType", name: "Unit" },
        body: {
          kind: "ScalaGroupExpression",
          expressions: [
            { kind: "ScalaIdentifierExpression", name: `val spark = SparkSession.builder().appName("${project.name}").master("local").getOrCreate()` },
            { kind: "ScalaIdentifierExpression", name: `spark.sparkContext.setLogLevel("WARN")` },
            { kind: "ScalaIdentifierExpression", name: `import spark.implicits._` },
            makeInitialDss(models),
            { kind: "ScalaIdentifierExpression", name: `if (args.size == 1) this.interpreter.run(args(0), spark, dss)` },
          ]
        }
      }
    ],
  }
}
