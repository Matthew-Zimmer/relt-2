import { Project } from "../../../project";
import { instructionsFor } from "../../relt/analysis/validInstructions";
import { vertexInfo } from "../../relt/analysis/vertex";
import { ReltModelDefinition } from "../../relt/types";
import { ScalaValExpression, datasetsType, thisExpression, ScalaObjectDefinition, ScalaBinaryOpExpression, ScalaExpression } from "../types";

export function makeInstructionSet(models: ReltModelDefinition[]): ScalaValExpression {
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
                kind: "ScalaOfType",
                type: {
                  kind: "ScalaIdentifierType",
                  name: "Instruction"
                },
                of: [{
                  kind: "ScalaDotType",
                  left: {
                    kind: "ScalaIdentifierType",
                    name: "DeltaTypes"
                  },
                  right: {
                    kind: "ScalaIdentifierType",
                    name: "Datasets"
                  }
                }]
              }
            ]
          }
        ],
      },
      args: models.map(makeInstructionSetForModel),
      hints: {
        indent: true,
      }
    }
  };
}

export function makeInstructionSetForModel(model: ReltModelDefinition): ScalaBinaryOpExpression {
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
      args: instructionsFor(model).map<ScalaExpression>(x => ({
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
            kind: "ScalaOfType",
            type: {
              kind: "ScalaIdentifierType",
              name: "Map"
            },
            of: [
              {
                kind: "ScalaStringType",
              },
              {
                kind: "ScalaIdentifierType",
                name: "Vertex"
              }
            ]
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
          types: [datasetsType()],
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
          types: [datasetsType()],
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

          ]
        }
      }
    ],
  }
}
