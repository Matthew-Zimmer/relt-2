import { match } from "ts-pattern";
import { ReltModelDefinition } from "../types";

export type Instruction =
  | RefreshInstruction
  | FetchInstruction
  | DeriveInstruction
  | StoreInstruction

export interface RefreshInstruction {
  kind: "RefreshInstruction";
  name: string;
  className: string;
  storageClassName: string;
  deltaClassName: string;
}

export interface FetchInstruction {
  kind: "FetchInstruction";
  name: string;
  className: string;
  storageClassName: string;
  deltaClassName: string;
}

export interface DeriveInstruction {
  kind: "DeriveInstruction";
  name: string;
  className: string;
}

export interface StoreInstruction {
  kind: "StoreInstruction";
  name: string;
  className: string;
  storageClassName: string;
}

export function instructionsFor(model: ReltModelDefinition): Instruction[] {
  if (isTypeModel(model)) return [];

  const isSource = isSourceModel(model);
  const isStored = isStoredModel(model);

  const hasRefreshInstruction = isSource;
  const hasFetchInstruction = isStored;
  const hasDeriveInstruction = !isSource;
  const hasStoreInstruction = isStored && !isSource;

  return [
    hasRefreshInstruction ? makeRefreshInstruction(model) : [],
    hasFetchInstruction ? makeFetchInstruction(model) : [],
    hasDeriveInstruction ? makeDeriveInstruction(model) : [],
    hasStoreInstruction ? makeStoreInstruction(model) : [],
  ].flat();
}

export function isSourceModel(model: ReltModelDefinition): boolean {
  return model.expression.kind === "ReltTypeObjectExpression";
}

export function isTypeModel(model: ReltModelDefinition): boolean {
  return model.modifiers.some(x => x.kind === "ReltTypeModelModifier");
}

export function isStoredModel(model: ReltModelDefinition): boolean {
  return model.modifiers.some(x => match(x.kind)
    .with("ReltDeltaModelModifier", () => true)
    .with("ReltIndexModelModifier", () => false)
    .with("ReltPostgresModelModifier", () => true)
    .with("ReltTypeModelModifier", () => false)
    .exhaustive()
  );
}

export function makeRefreshInstruction(model: ReltModelDefinition): RefreshInstruction {
  return {
    kind: "RefreshInstruction",
    name: "refresh",
    className: `Refresh${model.name}Instruction`,
    storageClassName: `${model.name}Storage`,
    deltaClassName: `${model.name}`,
  };
}

export function makeFetchInstruction(model: ReltModelDefinition): FetchInstruction {
  return {
    kind: "FetchInstruction",
    name: "fetch",
    className: `Fetch${model.name}Instruction`,
    storageClassName: `${model.name}Storage`,
    deltaClassName: `${model.name}`,
  };
}

export function makeDeriveInstruction(model: ReltModelDefinition): DeriveInstruction {
  return {
    kind: "DeriveInstruction",
    name: "derive",
    className: `Derive${model.name}Instruction`,
  };
}

export function makeStoreInstruction(model: ReltModelDefinition): StoreInstruction {
  return {
    kind: "StoreInstruction",
    name: "store",
    className: `Store${model.name}Instruction`,
    storageClassName: `${model.name}Storage`,
  };
}
