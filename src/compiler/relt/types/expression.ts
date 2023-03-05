import { ReltType } from "./type";

export type ReltExpression =
  | ReltPipeExpression
  | ReltWhereExpression
  | ReltSortExpression
  | ReltOverExpression
  | ReltJoinExpression
  | ReltUnionExpression
  | ReltWithExpression
  | ReltOrExpression
  | ReltAndExpression
  | ReltCmpExpression
  | ReltAddExpression
  | ReltMulExpression
  | ReltDotExpression
  | ReltStringExpression
  | ReltIntegerExpression
  | ReltFloatExpression
  | ReltBooleanExpression
  | ReltEnvVarExpression
  | ReltIdentifierExpression
  | ReltTypeObjectExpression
  | ReltGroupExpression
  | ReltCoalesceExpression

export interface ReltPipeExpression {
  kind: "ReltPipeExpression";
  left: ReltExpression;
  right: ReltExpression;
}

export interface ReltWhereExpression {
  kind: "ReltWhereExpression";
  head?: ReltExpression;
  condition: ReltExpression;
}

export interface ReltSortExpression {
  kind: "ReltSortExpression";
  head?: ReltExpression;
  columns: ReltIdentifierExpression[];
  op: "asc" | "desc";
}

export interface ReltOverExpression {
  kind: "ReltOverExpression";
  head?: ReltExpression;
  column: ReltExpression;
}

export interface ReltJoinExpression {
  kind: "ReltJoinExpression";
  head?: ReltExpression;
  op: "inner" | "left" | "right";
  on?: ReltGroupExpression;
  other: ReltExpression;
}

export interface ReltUnionExpression {
  kind: "ReltUnionExpression";
  head?: ReltExpression;
  other: ReltExpression;
}

export interface ReltWithExpression {
  kind: "ReltWithExpression";
  head?: ReltExpression;
  properties: ReltObjectProperty[];
}

export type ReltObjectProperty =
  | ReltAssignObjectProperty
  | ReltAsObjectProperty
  | ReltOpAssignObjectProperty
  | ReltRenameObjectProperty

export interface ReltAssignObjectProperty {
  kind: "ReltAssignObjectProperty";
  name: string;
  value: ReltExpression;
}

export interface ReltAsObjectProperty {
  kind: "ReltAsObjectProperty";
  name: string;
  type: ReltType;
}

export interface ReltOpAssignObjectProperty {
  kind: "ReltOpAssignObjectProperty";
  name: string;
  op: "??=";
  value: ReltExpression;
}

export interface ReltRenameObjectProperty {
  kind: "ReltRenameObjectProperty";
  name: string;
  value: ReltExpression;
}

export interface ReltCoalesceExpression {
  kind: "ReltCoalesceExpression";
  left: ReltExpression;
  op: "??";
  right: ReltExpression;
}

export interface ReltOrExpression {
  kind: "ReltOrExpression";
  left: ReltExpression;
  op: "or";
  right: ReltExpression;
}

export interface ReltAndExpression {
  kind: "ReltAndExpression";
  left: ReltExpression;
  op: "and";
  right: ReltExpression;
}

export interface ReltCmpExpression {
  kind: "ReltCmpExpression";
  left: ReltExpression;
  op: "==" | "!=" | "<=" | ">=" | "<" | ">";
  right: ReltExpression;
}

export interface ReltAddExpression {
  kind: "ReltAddExpression";
  left: ReltExpression;
  op: "+" | "-";
  right: ReltExpression;
}

export interface ReltMulExpression {
  kind: "ReltMulExpression";
  left: ReltExpression;
  op: "*" | "/" | "%";
  right: ReltExpression;
}

export interface ReltDotExpression {
  kind: "ReltDotExpression";
  left: ReltExpression;
  op: ".";
  right: ReltExpression;
}

export interface ReltStringExpression {
  kind: "ReltStringExpression";
  value: string;
}

export interface ReltIntegerExpression {
  kind: "ReltIntegerExpression";
  value: number;
}

export interface ReltFloatExpression {
  kind: "ReltFloatExpression";
  value: string;
}

export interface ReltBooleanExpression {
  kind: "ReltBooleanExpression";
  value: boolean;
}

export interface ReltEnvVarExpression {
  kind: "ReltEnvVarExpression";
  value: string;
}

export interface ReltIdentifierExpression {
  kind: "ReltIdentifierExpression";
  name: string;
}

export interface ReltTypeObjectExpression {
  kind: "ReltTypeObjectExpression";
  properties: { name: string, type: ReltType }[];
}

export interface ReltGroupExpression {
  kind: "ReltGroupExpression";
  value: ReltExpression;
}
