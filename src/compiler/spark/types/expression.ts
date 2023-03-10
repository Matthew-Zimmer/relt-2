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
  | ScalaStringExpression
  | ScalaBooleanExpression
  | ScalaIntegerExpression
  | ScalaFloatExpression
  | ScalaImportExpression
  | ScalaReturnExpression
  | ScalaTupleExpression
  | ScalaPartExpression

export interface ScalaVarExpression {
  kind: "ScalaVarExpression";
  name: string;
  value: ScalaExpression;
  visibility?: "private";
}

export interface ScalaValExpression {
  kind: "ScalaValExpression";
  name: string;
  value: ScalaExpression;
  visibility?: "private";
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
  hints?: {
    indent?: boolean;
  };
}

export interface ScalaDotExpression {
  kind: "ScalaDotExpression";
  left: ScalaExpression;
  right: ScalaExpression;
  hints?: {
    indent?: boolean;
  };
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

export interface ScalaStringExpression {
  kind: "ScalaStringExpression";
  value: string;
}

export interface ScalaBooleanExpression {
  kind: "ScalaBooleanExpression";
  value: boolean;
}

export interface ScalaIntegerExpression {
  kind: "ScalaIntegerExpression";
  value: number;
}

export interface ScalaFloatExpression {
  kind: "ScalaFloatExpression";
  value: string;
}

export interface ScalaImportExpression {
  kind: "ScalaImportExpression";
  value: ScalaExpression;
}

export interface ScalaReturnExpression {
  kind: "ScalaReturnExpression";
  value: ScalaExpression;
}

export interface ScalaTupleExpression {
  kind: "ScalaTupleExpression";
  values: ScalaExpression[];
}

export interface ScalaPartExpression {
  kind: "ScalaPartExpression";
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