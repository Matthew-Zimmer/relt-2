import { Project } from "../../../project";
import { ReltModelDefinition } from "../../relt/types";
import { ScalaValExpression, datasetsType, thisExpression, ScalaObjectDefinition } from "../types";

export function makeInstructionSet(models: ReltModelDefinition[]): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
    name: "instructionSet",
    value: {
      kind: "ScalaAppExpression",
      func: {
        kind: "ScalaIdentifierExpression",
        name: "Map",
        types: [],
      },
      args: []
    }
  };
}

export function makeVertices(models: ReltModelDefinition[]): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
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
      args: []
    }
  }
}

export function makeDag(): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
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
      }
    }
  };
}

export function makeInterpreter(): ScalaValExpression {
  return {
    kind: "ScalaValExpression",
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
      }
    }
  };
}

export function makeProjectEntrypoint(project: Project, models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: ``,
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
