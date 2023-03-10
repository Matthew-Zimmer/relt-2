import { match } from "ts-pattern";
import { instructionsFor } from "../../../relt/analysis/validInstructions";
import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";
import { convertDeriveInstruction } from "./derive";
import { convertFetchInstruction } from "./fetch";
import { convertRefreshInstruction } from "./refresh";
import { convertStoreInstruction } from "./store";

export function makeInstructions(models: ReltModelDefinition[]): ScalaObjectDefinition {
  const indices = Object.fromEntries(models.map((x, i) => [x.name, i]));
  return {
    kind: "ScalaObjectDefinition",
    name: "Instructions",
    properties: models.flatMap((x, i) => makeInstructionsFor(x, models.length, i + 1, indices)),
  };
}

export function makeInstructionsFor(model: ReltModelDefinition, count: number, idx: number, indices: Record<string, number>): ScalaObjectDefinition[] {
  const instructions = instructionsFor(model, indices);
  return instructions.map(ins => (
    match(ins)
      .with({ kind: "DeriveInstruction" }, x => convertDeriveInstruction(x, count, idx))
      .with({ kind: "FetchInstruction" }, x => convertFetchInstruction(x, count, idx))
      .with({ kind: "RefreshInstruction" }, x => convertRefreshInstruction(x, count, idx))
      .with({ kind: "StoreInstruction" }, x => convertStoreInstruction(x, count, idx))
      .exhaustive()
  ));
}
