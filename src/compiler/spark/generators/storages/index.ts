import { ReltModelDefinition } from "../../../relt/types";
import { ScalaObjectDefinition } from "../../types";

export function makeStorages(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "Storages",
    properties: [],
  };
}
