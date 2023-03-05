import { ScalaExpression } from "./expression";

export type ScalaDefinition =
  | ScalaObjectDefinition
  | ScalaClassDefinition
  | ScalaTraitDefinition
  | ScalaTypeDefinition

export interface ScalaObjectDefinition {
  kind: "ScalaObjectDefinition";
  name: string;
  extends?: string;
  properties: (ScalaExpression | ScalaDefinition)[];
}

export interface ScalaClassDefinition {
  kind: "ScalaClassDefinition";
}

export interface ScalaTraitDefinition {
  kind: "ScalaTraitDefinition";
}

export interface ScalaTypeDefinition {
  kind: "ScalaTypeDefinition";
}
