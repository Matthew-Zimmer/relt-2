import { throws } from "../../../../errors";
import { isStoredModel } from "../../../relt/analysis/validInstructions";
import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";
import { makePostgresStorage } from "./postgres";

export function makeStorages(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "Storages",
    properties: models.filter(isStoredModel).map(makeStorage),
  };
}

export function makeStorage(model: ReltModelDefinition): ScalaObjectDefinition {
  const hasPostgresStorage = model.modifiers.some(x => x.kind === "ReltPostgresModelModifier");
  const hasDeltaStorage = model.modifiers.some(x => x.kind === "ReltDeltaModelModifier");

  if (hasPostgresStorage)
    return makePostgresStorage(model);
  else if (hasDeltaStorage)
    throws(`TODO delta storage`);
  else
    throws(`BAD: not storage kind but tried to create storage: ${model.name}`);
}
