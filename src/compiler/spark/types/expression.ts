import { ScalaType } from "./type";

export type ScalaExpression =
  | ScalaVarExpression
  | ScalaValExpression
  | ScalaBinaryOpExpression
  | ScalaAppExpression
  | ScalaDotExpression
  | ScalaIdentifierExpression
  | ScalaDefExpression
  | ScalaGroupExpression
  | ScalaNewExpression




export interface ScalaVarExpression {
  kind: "ScalaVarExpression";
  name: string;
  value: ScalaExpression;
}

export interface ScalaValExpression {
  kind: "ScalaValExpression";
  name: string;
  value: ScalaExpression;
}

export interface ScalaBinaryOpExpression {
  kind: "ScalaBinaryOpExpression";
  left: ScalaExpression;
  op: string;
  right: ScalaExpression;
}

export interface ScalaAppExpression {
  kind: "ScalaAppExpression";
  func: ScalaExpression;
  args: ScalaExpression[];
}

export interface ScalaDotExpression {
  kind: "ScalaDotExpression";
  left: ScalaExpression;
  right: ScalaExpression;
}

export interface ScalaIdentifierExpression {
  kind: "ScalaIdentifierExpression";
  name: string;
  types?: ScalaType[];
}

export interface ScalaDefExpression {
  kind: "ScalaDefExpression";
  name: string;
  args: { name: string, type: ScalaType }[];
  returnType: ScalaType;
  body: ScalaExpression;
}

export interface ScalaGroupExpression {
  kind: "ScalaGroupExpression";
  expressions: ScalaExpression[];
}

export interface ScalaNewExpression {
  kind: "ScalaNewExpression";
  value: ScalaExpression;
}

export function thisExpression(right: ScalaExpression): ScalaExpression {
  return {
    kind: "ScalaDotExpression",
    left: {
      kind: "ScalaIdentifierExpression",
      name: "this"
    },
    right
  };
}