import { ReltModelDefinition } from "../../relt/types";
import { ScalaCaseClassDefinition, ScalaObjectDefinition } from "../types";
import { makeType } from "./types";

export function makeDeltaTypes(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "DeltaTypes",
    properties: [
      {
        kind: "ScalaTypeDefinition",
        name: "Datasets",
        type: {
          kind: "ScalaTupleType",
          types: models.map(model => ({
            kind: "ScalaOfType",
            type: {
              kind: "ScalaIdentifierType",
              name: "Dataset"
            },
            of: [{
              kind: "ScalaIdentifierType",
              name: model.name,
            }]
          })),
        }
      },
      ...models.map(makeDeltaType),
    ],
  };
}

export const deltaColumnName = "__delta_state_kind";

export function makeDeltaType(model: ReltModelDefinition): ScalaCaseClassDefinition {
  const baseTable = makeType(model);

  return {
    ...baseTable,
    properties: [
      ...baseTable.properties,
      {
        name: deltaColumnName,
        type: {
          kind: "ScalaDotType",
          left: {
            kind: "ScalaIdentifierType",
            name: "DeltaState"
          },
          right: {
            kind: "ScalaIdentifierType",
            name: "Kind"
          },
        },
      },
    ]
  };
}
