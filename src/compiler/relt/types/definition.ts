import { ReltExpression, ReltStringExpression, ReltEnvVarExpression, ReltIdentifierExpression } from "./expression";

export type ReltDefinition =
  | ReltModelDefinition

export interface ReltModelDefinition {
  kind: "ReltModelDefinition";
  modifiers: ReltModelModifier[];
  name: string;
  expression: ReltExpression;
}

export type ReltModelModifier =
  | ReltDeltaModelModifier
  | ReltPostgresModelModifier
  | ReltIndexModelModifier
  | ReltTypeModelModifier
  | ReltExternalModelModifier

export interface ReltDeltaModelModifier {
  kind: "ReltDeltaModelModifier";
  value: ReltStringExpression | ReltEnvVarExpression;
}

export interface ReltPostgresModelModifier {
  kind: "ReltPostgresModelModifier";
  value: ReltStringExpression | ReltEnvVarExpression;
}

export interface ReltIndexModelModifier {
  kind: "ReltIndexModelModifier";
  value: ReltStringExpression | ReltEnvVarExpression;
  on: ReltStringExpression;
}

export interface ReltTypeModelModifier {
  kind: "ReltTypeModelModifier";
}

export interface ReltExternalModelModifier {
  kind: "ReltExternalModelModifier";
  value: ReltStringExpression | ReltEnvVarExpression;
  using: ReltIdentifierExpression[];
}
