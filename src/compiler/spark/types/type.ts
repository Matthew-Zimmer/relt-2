export type ScalaType =
  | ScalaDoubleType
  | ScalaIntType
  | ScalaBooleanType
  | ScalaStringType
  | ScalaIdentifierType
  | ScalaDotType
  | ScalaOfType
  | ScalaDateType
  | ScalaTupleType

export interface ScalaDoubleType {
  kind: "ScalaDoubleType"
}

export interface ScalaIntType {
  kind: "ScalaIntType"
}

export interface ScalaBooleanType {
  kind: "ScalaBooleanType"
}

export interface ScalaStringType {
  kind: "ScalaStringType"
}

export interface ScalaIdentifierType {
  kind: "ScalaIdentifierType",
  name: string
}

export interface ScalaDotType {
  kind: "ScalaDotType",
  left: ScalaType,
  right: ScalaType
}

export interface ScalaOfType {
  kind: "ScalaOfType",
  type: ScalaType,
  of: ScalaType[]
}

export interface ScalaTupleType {
  kind: "ScalaTupleType",
  types: ScalaType[],
}

export interface ScalaDateType {
  kind: "ScalaDateType"
}

export function datasetsType(): ScalaType {
  return {
    kind: "ScalaDotType",
    left: {
      kind: "ScalaIdentifierType",
      name: "DeltaTypes"
    },
    right: {
      kind: "ScalaIdentifierType",
      name: "Datasets"
    },
  };
}
