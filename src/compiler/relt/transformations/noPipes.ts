import { Transformation } from ".";
import { assertExpectation } from "../../../errors";
import { ReltDefinition, ReltExpression, ReltModelDefinition } from "../types";

export const noPipes = {
  name: "noPipes",
  transform(m) {
    let c = 0;

    const def = (x: ReltDefinition): ReltDefinition[] => {
      switch (x.kind) {
        case "ReltModelDefinition": {
          const [e, d] = expr(x.expression);
          return [...d, { ...x, expression: e }];
        }
        default:
          return [x];
      }
    };

    const makeHead = (l: ReltExpression, r: ReltExpression): ReltExpression => {
      switch (r.kind) {
        case "ReltWhereExpression":
        case "ReltUnionExpression":
        case "ReltOverExpression":
        case "ReltJoinExpression":
        case "ReltSortExpression":
        case "ReltWithExpression":
          return { ...r, head: l };
        default:
          assertExpectation(false, `Cannot unpipe ${l.kind}`); /* hint for typescript */ throw '';
      }
    };

    const expr = (x: ReltExpression): [ReltExpression, ReltModelDefinition[]] => {
      switch (x.kind) {
        case "ReltPipeExpression": {
          // we need to make a model!
          if (x.left.kind === "ReltPipeExpression") {
            const [e, ds] = expr(x.left);

            const name = `RELT_${c++}`;

            const d = {
              kind: "ReltModelDefinition",
              modifiers: [],
              name,
              expression: e,
            } satisfies ReltModelDefinition;

            return [makeHead({ kind: "ReltIdentifierExpression", name }, x.right), [...ds, d]];
          }
          // we can be inline
          else {
            return [makeHead(x.left, x.right), []];
          }
        }
        default:
          return pull(expr, applyToChildren)(x);
      }
    };

    return { kind: "ReltModule", definitions: m.definitions.flatMap(def) };
  },
} satisfies Transformation;
