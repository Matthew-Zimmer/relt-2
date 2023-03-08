import { ScalaExpression } from "./expression";
import { ScalaType } from "./type";

export type ScalaDefinition =
  | ScalaObjectDefinition
  | ScalaClassDefinition
  | ScalaTraitDefinition
  | ScalaTypeDefinition
  | ScalaCaseClassDefinition

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
  name: string;
  type: ScalaType;
}

export interface ScalaCaseClassDefinition {
  kind: "ScalaCaseClassDefinition";
  name: string;
  properties: { name: string, type: ScalaType }[];
}
