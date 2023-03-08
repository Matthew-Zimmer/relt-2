import { Transformation } from ".";
import { assertExpectation } from "../../../errors";
import { ReltDefinition, ReltExpression, ReltGroupExpression, ReltIdentifierExpression, ReltModelDefinition, ReltObjectProperty } from "../types";

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
        case "ReltWhereExpression": {
          const h = x.head === undefined ? [undefined, []] as const : expr(x.head);
          const c = expr(x.condition);
          return [{ ...x, head: h[0], condition: c[0] }, [...h[1], ...c[1]]];
        }
        case "ReltSortExpression": {
          const h = x.head === undefined ? [undefined, []] as const : expr(x.head);
          const c = x.columns.reduce<[ReltIdentifierExpression[], ReltModelDefinition[]]>((p, c) => {
            const x = expr(c);
            return [[...p[0], x[0] as ReltIdentifierExpression], [...p[1], ...x[1]]];
          }, [[], []]);
          return [{ ...x, head: h[0], columns: c[0] }, [...h[1], ...c[1]]];
        }
        case "ReltOverExpression": {
          const h = x.head === undefined ? [undefined, []] as const : expr(x.head);
          const c = expr(x.column);
          return [{ ...x, head: h[0], column: c[0] }, [...h[1], ...c[1]]];
        }
        case "ReltJoinExpression": {
          const h = x.head === undefined ? [undefined, []] as const : expr(x.head);
          const cond = x.on === undefined ? [undefined, []] as const : expr(x.on);
          const o = expr(x.other);
          return [{ ...x, head: h[0], on: cond[0] as ReltGroupExpression, other: o[0] }, [...h[1], ...cond[1], ...o[1]]];
        }
        case "ReltUnionExpression": {
          const h = x.head === undefined ? [undefined, []] as const : expr(x.head);
          const o = expr(x.other);
          return [{ ...x, head: h[0], other: o[0] }, [...h[1], ...o[1]]];
        }
        case "ReltWithExpression": {
          const h = x.head === undefined ? [undefined, []] as const : expr(x.head);
          const p = x.properties.reduce<[ReltObjectProperty[], ReltModelDefinition[]]>((p, c) => {
            switch (c.kind) {
              case "ReltAsObjectProperty":
                return [[...p[0], c], p[1]];
              case "ReltAssignObjectProperty":
              case "ReltOpAssignObjectProperty":
              case "ReltRenameObjectProperty": {
                const v = expr(c.value);
                return [[...p[0], { ...c, value: v[0] }], [...p[1], ...v[1]]];
              }
            }
          }, [[], []]);
          return [{ ...x, head: h[0], properties: p[0] }, [...h[1], ...p[1]]];
        }
        case "ReltOrExpression":
        case "ReltAndExpression":
        case "ReltCmpExpression":
        case "ReltAddExpression":
        case "ReltMulExpression":
        case "ReltDotExpression":
        case "ReltCoalesceExpression": {
          const l = expr(x.left);
          const r = expr(x.right);
          return [{ ...x, left: l[0], right: r[0] }, [...l[1], ...r[1]]];
        }
        case "ReltStringExpression":
        case "ReltIntegerExpression":
        case "ReltFloatExpression":
        case "ReltBooleanExpression":
        case "ReltEnvVarExpression":
        case "ReltIdentifierExpression":
        case "ReltTypeObjectExpression": {
          return [{ ...x, }, []];
        }
        case "ReltGroupExpression": {
          const v = expr(x.value);
          return [{ ...x, value: v[0] }, v[1]];
        }
      }
    };

    return { kind: "ReltModule", definitions: m.definitions.flatMap(def) };
  },
} satisfies Transformation;
