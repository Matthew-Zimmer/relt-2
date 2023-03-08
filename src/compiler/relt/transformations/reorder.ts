import { Transformation } from ".";
import { assertInvariant } from "../../../errors";
import { deps } from "../analysis/vertex";
import { ReltDefinition } from "../types";

export const reorder = {
  name: "reorder",
  transform(x) {
    const def = (x: ReltDefinition): ReltDefinition => {
      switch (x.kind) {
        case "ReltModelDefinition": {
          deps.addVertex(x.name);
          return x;
        }
      }
    };
    return { kind: "ReltModule", definitions: x.definitions.map(def) };
  },
} satisfies Transformation;
