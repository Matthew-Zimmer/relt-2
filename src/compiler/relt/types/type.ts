export type ReltType =
  | ReltStringType
  | ReltBooleanType
  | ReltIntegerType
  | ReltFloatType
  | ReltStructType
  | ReltIdentifierType
  | ReltOptionalType
  | ReltArrayType
  | ReltJsonType
  | ReltDateType

export interface ReltStringType {
  kind: "ReltStringType";
}

export interface ReltBooleanType {
  kind: "ReltBooleanType";
}

export interface ReltIntegerType {
  kind: "ReltIntegerType";
}

export interface ReltFloatType {
  kind: "ReltFloatType";
}

export interface ReltStructType {
  kind: "ReltStructType";
  properties: { name: string, type: ReltType }[];
}

export interface ReltIdentifierType {
  kind: "ReltIdentifierType";
  name: string;
}

export interface ReltOptionalType<T extends ReltType = ReltType> {
  kind: "ReltOptionalType";
  of: T;
}

export interface ReltArrayType {
  kind: "ReltArrayType";
  of: ReltType;
}

export interface ReltJsonType {
  kind: "ReltJsonType";
  of: ReltType;
}

export interface ReltDateType {
  kind: "ReltDateType";
  fmt?: string;
}
