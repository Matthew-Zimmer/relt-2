import { ScalaDefExpression, ScalaExpression } from "../../types";

export function makeReadStorage(name: string, expressions: ScalaExpression[]): ScalaDefExpression {
  return {
    kind: "ScalaDefExpression",
    name: "read",
    returnType: {
      kind: "ScalaOfType",
      type: {
        kind: "ScalaIdentifierType",
        name: "Dataset"
      },
      of: [{
        kind: "ScalaDotType",
        left: {
          kind: "ScalaIdentifierType",
          name: "Types"
        },
        right: {
          kind: "ScalaIdentifierType",
          name: name,
        }
      }]
    },
    args: [{
      name: "spark",
      type: {
        kind: "ScalaIdentifierType",
        name: "SparkSession"
      }
    }],
    body: {
      kind: "ScalaGroupExpression",
      expressions
    },
  };
}

export function makeWriteStorage(name: string, expressions: ScalaExpression[]): ScalaDefExpression {
  return {
    kind: "ScalaDefExpression",
    name: "write",
    returnType: {
      kind: "ScalaIdentifierType",
      name: "Unit",
    },
    args: [{
      name: "spark",
      type: {
        kind: "ScalaIdentifierType",
        name: "SparkSession"
      }
    }, {
      name: "ds",
      type: {
        kind: "ScalaOfType",
        type: {
          kind: "ScalaIdentifierType",
          name: "Dataset"
        },
        of: [{
          kind: "ScalaDotType",
          left: {
            kind: "ScalaIdentifierType",
            name: "DeltaTypes"
          },
          right: {
            kind: "ScalaIdentifierType",
            name: name,
          }
        }]
      }
    }],
    body: {
      kind: "ScalaGroupExpression",
      expressions
    },
  };
}
