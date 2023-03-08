import { match, P } from "ts-pattern";
import { assertDefined, assertExpectation, assertInvariant, assertKind } from "../../../errors";
import { formatRelt } from "../format";
import { ReltBooleanType, ReltDateType, ReltExpression, ReltFloatType, ReltIntegerType, ReltModelDefinition, ReltOptionalType, ReltStringType, ReltStructType, ReltType } from "../types";

export const types = {
  bool: { kind: "ReltBooleanType" } satisfies ReltBooleanType,
  int: { kind: "ReltIntegerType" } satisfies ReltIntegerType,
  float: { kind: "ReltFloatType" } satisfies ReltFloatType,
  date: { kind: "ReltDateType" } satisfies ReltDateType,
  string: { kind: "ReltStringType" } satisfies ReltStringType,
  opt: <T extends ReltType>(of: T) => ({ kind: "ReltOptionalType", of }) satisfies ReltOptionalType<T>,
};

export function typeEquals(l: ReltType, r: ReltType): boolean {
  return match([l, r])
    .with([types.int, types.int], () => true)
    .with([types.float, types.float], () => true)
    .with([types.bool, types.bool], () => true)
    .with([types.string, types.string], () => true)
    .with([types.date, types.date], () => true)
    .with([{ kind: "ReltOptionalType", of: P.select("l") }, { kind: "ReltOptionalType", of: P.select("r") }], ({ l, r }) => typeEquals(l, r))
    .with([{ kind: "ReltArrayType", of: P.select("l") }, { kind: "ReltArrayType", of: P.select("r") }], ({ l, r }) => typeEquals(l, r))
    .with([{ kind: "ReltIdentifierType", name: P.select("l") }, { kind: "ReltIdentifierType", name: P.select("r") }], ({ l, r }) => l === r)
    .with([{ kind: "ReltStructType", properties: P.select("l") }, { kind: "ReltStructType", properties: P.select("r") }], ({ l, r }) => {
      if (l.length !== r.length) return false;
      const lP = new Map(l.map(x => [x.name, x.type]));
      const rP = new Map(r.map(x => [x.name, x.type]));
      if (l.some(x => !rP.has(x.name))) return false;
      if (r.some(x => !lP.has(x.name))) return false;
      return l.every(x => typeEquals(lP.get(x.name)!, rP.get(x.name)!));
    })
    .otherwise(() => false)
}

export function mergeStructs(l: ReltStructType, r: ReltStructType, options?: { allowOverride: boolean }): ReltStructType {
  const { allowOverride } = options ?? { allowOverride: false };
  const properties: ReltStructType['properties'] = [...l.properties];
  for (const p of r.properties) {
    if (!allowOverride)
      assertExpectation(properties.findIndex(x => x.name === p.name) === -1, `${p.name} is already in struct`);
    properties.push(p);
  }
  return { kind: "ReltStructType", properties };
}

export function overload(args: ReltType[], params: ReltType[], res: ReltType): ReltType | undefined {
  if (args.length !== params.length) return undefined;
  if (args.every((_, i) => typeEquals(args[i], params[i])))
    return res;
  return undefined;
}

export function opTypeCheck(l: ReltType, op: string, r: ReltType): ReltType | undefined {
  const { int, bool, date, float, opt, string } = types;

  return match([op, l, r])
    .with(["??", { kind: "ReltOptionalType", of: P.select("l") }, P.select("r")], ({ l, r }) => typeEquals(l, r) ? l : undefined)
    .with(["or", bool, bool], () => bool)
    .with(["or", opt(bool), bool], () => opt(bool))
    .with(["or", bool, opt(bool)], () => opt(bool))
    .with(["or", opt(bool), opt(bool)], () => opt(bool))
    .with(["and", bool, bool], () => bool)
    .with(["and", opt(bool), bool], () => opt(bool))
    .with(["and", bool, opt(bool)], () => opt(bool))
    .with(["and", opt(bool), opt(bool)], () => opt(bool))
    .with(["==", P.select("l"), P.select("r")], ({ l, r }) => typeEquals(l, r) ? bool : undefined)
    .with(["==", { kind: "ReltOptionalType", of: P.select("l") }, P.select("r")], ({ l, r }) => typeEquals(l, r) ? opt(bool) : undefined)
    .with(["==", P.select("l"), { kind: "ReltOptionalType", of: P.select("r") }], ({ l, r }) => typeEquals(l, r) ? opt(bool) : undefined)
    .with(["==", { kind: "ReltOptionalType", of: P.select("l") }, { kind: "ReltOptionalType", of: P.select("r") }], ({ l, r }) => typeEquals(l, r) ? opt(bool) : undefined)
    .with(["!=", P.select("l"), P.select("r")], ({ l, r }) => typeEquals(l, r) ? bool : undefined)
    .with(["!=", { kind: "ReltOptionalType", of: P.select("l") }, P.select("r")], ({ l, r }) => typeEquals(l, r) ? opt(bool) : undefined)
    .with(["!=", P.select("l"), { kind: "ReltOptionalType", of: P.select("r") }], ({ l, r }) => typeEquals(l, r) ? opt(bool) : undefined)
    .with(["!=", { kind: "ReltOptionalType", of: P.select("l") }, { kind: "ReltOptionalType", of: P.select("r") }], ({ l, r }) => typeEquals(l, r) ? opt(bool) : undefined)
    .with(["<=", int, int], () => bool)
    .with(["<=", opt(int), int], () => opt(bool))
    .with(["<=", int, opt(int)], () => opt(bool))
    .with(["<=", opt(int), opt(int)], () => opt(bool))
    .with(["<=", float, float], () => bool)
    .with(["<=", opt(float), float], () => opt(bool))
    .with(["<=", float, opt(float)], () => opt(bool))
    .with(["<=", opt(float), opt(float)], () => opt(bool))
    .with(["<=", date, date], () => bool)
    .with(["<=", opt(date), date], () => opt(bool))
    .with(["<=", date, opt(date)], () => opt(bool))
    .with(["<=", opt(date), opt(date)], () => opt(bool))
    .with([">=", int, int], () => bool)
    .with([">=", opt(int), int], () => opt(bool))
    .with([">=", int, opt(int)], () => opt(bool))
    .with([">=", opt(int), opt(int)], () => opt(bool))
    .with([">=", float, float], () => bool)
    .with([">=", opt(float), float], () => opt(bool))
    .with([">=", float, opt(float)], () => opt(bool))
    .with([">=", opt(float), opt(float)], () => opt(bool))
    .with([">=", date, date], () => bool)
    .with([">=", opt(date), date], () => opt(bool))
    .with([">=", date, opt(date)], () => opt(bool))
    .with([">=", opt(date), opt(date)], () => opt(bool))
    .with(["<", int, int], () => bool)
    .with(["<", opt(int), int], () => opt(bool))
    .with(["<", int, opt(int)], () => opt(bool))
    .with(["<", opt(int), opt(int)], () => opt(bool))
    .with(["<", float, float], () => bool)
    .with(["<", opt(float), float], () => opt(bool))
    .with(["<", float, opt(float)], () => opt(bool))
    .with(["<", opt(float), opt(float)], () => opt(bool))
    .with(["<", date, date], () => bool)
    .with(["<", opt(date), date], () => opt(bool))
    .with(["<", date, opt(date)], () => opt(bool))
    .with(["<", opt(date), opt(date)], () => opt(bool))
    .with([">", int, int], () => bool)
    .with([">", opt(int), int], () => opt(bool))
    .with([">", int, opt(int)], () => opt(bool))
    .with([">", opt(int), opt(int)], () => opt(bool))
    .with([">", float, float], () => bool)
    .with([">", opt(float), float], () => opt(bool))
    .with([">", float, opt(float)], () => opt(bool))
    .with([">", opt(float), opt(float)], () => opt(bool))
    .with([">", date, date], () => bool)
    .with([">", opt(date), date], () => opt(bool))
    .with([">", date, opt(date)], () => opt(bool))
    .with([">", opt(date), opt(date)], () => opt(bool))
    .with(["+", int, int], () => int)
    .with(["+", opt(int), int], () => opt(int))
    .with(["+", int, opt(int)], () => opt(int))
    .with(["+", opt(int), opt(int)], () => opt(int))
    .with(["+", float, float], () => float)
    .with(["+", opt(float), float], () => opt(float))
    .with(["+", float, opt(float)], () => opt(float))
    .with(["+", opt(float), opt(float)], () => opt(float))
    .with(["+", string, string], () => string)
    .with(["+", opt(string), string], () => opt(string))
    .with(["+", string, opt(string)], () => opt(string))
    .with(["+", opt(string), opt(string)], () => opt(string))
    .with(["-", int, int], () => int)
    .with(["-", opt(int), int], () => opt(int))
    .with(["-", int, opt(int)], () => opt(int))
    .with(["-", opt(int), opt(int)], () => opt(int))
    .with(["-", float, float], () => float)
    .with(["-", opt(float), float], () => opt(float))
    .with(["-", float, opt(float)], () => opt(float))
    .with(["-", opt(float), opt(float)], () => opt(float))
    .with(["*", int, int], () => int)
    .with(["*", opt(int), int], () => opt(int))
    .with(["*", int, opt(int)], () => opt(int))
    .with(["*", opt(int), opt(int)], () => opt(int))
    .with(["*", float, float], () => float)
    .with(["*", opt(float), float], () => opt(float))
    .with(["*", float, opt(float)], () => opt(float))
    .with(["*", opt(float), opt(float)], () => opt(float))
    .with(["/", int, int], () => int)
    .with(["/", opt(int), int], () => opt(int))
    .with(["/", int, opt(int)], () => opt(int))
    .with(["/", opt(int), opt(int)], () => opt(int))
    .with(["/", float, float], () => float)
    .with(["/", opt(float), float], () => opt(float))
    .with(["/", float, opt(float)], () => opt(float))
    .with(["/", opt(float), opt(float)], () => opt(float))
    .with(["%", int, int], () => int)
    .with(["%", opt(int), int], () => opt(int))
    .with(["%", int, opt(int)], () => opt(int))
    .with(["%", opt(int), opt(int)], () => opt(int))
    .with(["%", float, float], () => float)
    .with(["%", opt(float), float], () => opt(float))
    .with(["%", float, opt(float)], () => opt(float))
    .with(["%", opt(float), opt(float)], () => opt(float))
    .otherwise(() => undefined)
}

export class TypeChecker {
  private models = new Map<string, ReltStructType>();

  typeCheck(x: ReltModelDefinition): ReltStructType {
    if (this.models.has(x.name))
      return this.models.get(x.name)!;

    console.log(`Type checking: ${x.name}`);

    const t = this.typeCheckExpr(x.expression);

    assertKind(t, "ReltStructType", "");

    this.models.set(x.name, t);
    this.ctx.clear();
    this.workingType = undefined;

    return t as ReltStructType;
  }

  private ctx = new Map<string, ReltType>();
  private workingType?: ReltStructType = undefined;
  private setWorkingType(x: ReltStructType) {
    this.workingType = { ...x };
    this.ctx.clear();
    x.properties.forEach(x => {
      assertExpectation(!this.ctx.has(x.name), `${x.name} is already defined`);
      this.ctx.set(x.name, x.type);
    });
  }

  typeCheckExpr(x: ReltExpression): ReltType {
    switch (x.kind) {
      case "ReltBooleanExpression": return types.bool;
      case "ReltFloatExpression": return types.float;
      case "ReltStringExpression": return types.string;
      case "ReltEnvVarExpression": return types.string;
      case "ReltIntegerExpression": return types.int;
      case "ReltGroupExpression": return this.typeCheckExpr(x.value);
      case "ReltIdentifierExpression": {
        if (this.models.has(x.name))
          return this.models.get(x.name)!;

        const type = this.ctx.get(x.name);
        assertDefined(type, `Unknown variable ${x.name}`);
        return type;
      }
      case "ReltTypeObjectExpression":
        return { kind: "ReltStructType", properties: x.properties };
      case "ReltDotExpression": {
        const l = this.typeCheckExpr(x.left);
        assertKind(l, 'ReltStructType', `Left side of a dot needs to be a struct type`);
        const oldCtx = new Map(this.ctx);
        this.ctx = new Map(l.properties.map(x => [x.name, x.type]));
        const r = this.typeCheckExpr(x.right);
        this.ctx = oldCtx;
        return r;
      }

      case "ReltOrExpression":
      case "ReltAndExpression":
      case "ReltAddExpression":
      case "ReltCmpExpression":
      case "ReltMulExpression":
      case "ReltCoalesceExpression": {
        const l = this.typeCheckExpr(x.left);
        const r = this.typeCheckExpr(x.right);
        const t = opTypeCheck(l, x.op, r);
        assertDefined(t, `Bad ${x.op} with (${l.kind}, ${r.kind})`);
        return t;
      }

      case "ReltPipeExpression": {
        const l = this.typeCheckExpr(x.left);
        assertKind(l, 'ReltStructType', `Left side of pipe needs to be a struct type`);

        this.setWorkingType(l);

        const r = this.typeCheckExpr(x.right);
        assertKind(r, 'ReltStructType', `Right side of pipe needs to be a struct type`);

        // is this needed?
        this.setWorkingType(r);

        return r;
      }

      case "ReltJoinExpression": {
        assertDefined(this.workingType, `Did you forget to pipe in the left model?`);

        const r = this.typeCheckExpr(x.other);
        assertKind(r, 'ReltStructType', `Right side of join needs to be a struct type`);

        return mergeStructs(this.workingType, r);
      }
      case "ReltSortExpression": {
        assertDefined(this.workingType, `Did you forget to pipe in the left model?`);

        const oldCtx = new Map(this.ctx);
        this.ctx = new Map(this.workingType.properties.map(x => [x.name, x.type]));
        x.columns.forEach(x => this.typeCheckExpr(x));
        this.ctx = oldCtx;
        return this.workingType;
      }
      case "ReltWhereExpression": {
        assertDefined(this.workingType, `Did you forget to pipe in the left model?`);

        const oldCtx = new Map(this.ctx);
        this.ctx = new Map(this.workingType.properties.map(x => [x.name, x.type]));
        const c = this.typeCheckExpr(x.condition);
        assertExpectation(typeEquals(c, types.bool), `Where condition needs to be a bool got ${formatRelt(c)}`);
        this.ctx = oldCtx;
        return this.workingType;
      }
      case "ReltUnionExpression": {
        assertDefined(this.workingType, `Did you forget to pipe in the left model?`);

        const r = this.typeCheckExpr(x.other);
        assertKind(r, 'ReltStructType', `Right side of union needs to be a struct type`);
        assertExpectation(typeEquals(this.workingType, r), `Cannot union non equal struct types`);
        return this.workingType;
      }
      case "ReltWithExpression": {
        assertDefined(this.workingType, `Did you forget to pipe in the left model?`);

        const properties = x.properties.map(x => {
          switch (x.kind) {
            case "ReltAsObjectProperty":
              return { name: x.name, type: x.type };
            case "ReltAssignObjectProperty":
            case "ReltRenameObjectProperty":
            case "ReltOpAssignObjectProperty":
              return { name: x.name, type: this.typeCheckExpr(x.value) };
          }
        });
        return mergeStructs(this.workingType, { kind: "ReltStructType", properties }, { allowOverride: true });
      }
      case "ReltOverExpression":
        throw `TODO`;
    }
  }
}

export const tc = new TypeChecker();
