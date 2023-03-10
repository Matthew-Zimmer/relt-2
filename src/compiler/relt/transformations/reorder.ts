import { Transformation } from ".";
import { deps } from "../analysis/vertex";
import { ReltDefinition, ReltExpression, ReltExternalModelModifier } from "../types";

export const reorder = {
  name: "reorder",
  transform(x) {
    const modelNames = new Set(x.definitions.map(x => x.name));

    const def = (x: ReltDefinition): ReltDefinition => {
      switch (x.kind) {
        case "ReltModelDefinition": {
          deps.addVertex(x.name);
          expr(x.expression, x.name);
          const mod = x.modifiers.find(x => x.kind === "ReltExternalModelModifier") as ReltExternalModelModifier | undefined;
          if (mod !== undefined) {
            mod.using.forEach(y => deps.addEdge(y.name, x.name));
          }
          return x;
        }
      }
    };

    const expr = (x: ReltExpression, name: string) => {
      switch (x.kind) {
        case "ReltIdentifierExpression":
          if (modelNames.has(x.name))
            deps.addEdge(x.name, name);
          break;

        case "ReltStringExpression":
        case "ReltIntegerExpression":
        case "ReltFloatExpression":
        case "ReltBooleanExpression":
        case "ReltEnvVarExpression":
        case "ReltTypeObjectExpression":
        case "ReltWhereExpression":
        case "ReltSortExpression":
        case "ReltOverExpression":
        case "ReltWithExpression":
          break;

        case "ReltGroupExpression":
          expr(x.value, name);
          break;

        case "ReltOrExpression":
        case "ReltAndExpression":
        case "ReltCmpExpression":
        case "ReltAddExpression":
        case "ReltMulExpression":
        case "ReltCoalesceExpression":
        case "ReltDotExpression":
        case "ReltPipeExpression":
          expr(x.left, name);
          expr(x.right, name);
          break;

        case "ReltJoinExpression":
          expr(x.other, name);
          break;

        case "ReltUnionExpression":
          expr(x.other, name);
          break;
      }
    };

    return { kind: "ReltModule", definitions: x.definitions.map(def) };
  },
} satisfies Transformation;
