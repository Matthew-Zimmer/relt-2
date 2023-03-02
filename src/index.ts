import { writeFileSync } from "fs";
import { readFile, writeFile } from "fs/promises";
import { parser } from "./grammar";
import { scalaTemplate } from "./stage0";

export interface Module {
  kind: "Module";
  definitions: Definition[];
}

export type Type =
  | StringType
  | BooleanType
  | IntegerType
  | FloatType
  | StructType
  | IdentifierType
  | OptionalType
  | ArrayType
  | JsonType
  | DateType

export interface StringType {
  kind: "StringType";
}

export interface BooleanType {
  kind: "BooleanType";
}

export interface IntegerType {
  kind: "IntegerType";
}

export interface FloatType {
  kind: "FloatType";
}

export interface StructType {
  kind: "StructType";
  properties: { name: string, type: Type }[];
}

export interface IdentifierType {
  kind: "IdentifierType";
  name: string;
}

export interface OptionalType {
  kind: "OptionalType";
  of: Type;
}

export interface ArrayType {
  kind: "ArrayType";
  of: Type;
}

export interface JsonType {
  kind: "JsonType";
  of: Type;
}

export interface DateType {
  kind: "DateType";
  fmt?: string;
}

export type Definition =
  | ModelDefinition

export interface ModelDefinition {
  kind: "ModelDefinition";
  modifiers: ModelModifier[];
  name: string;
  expression: Expression;
}

export type ModelModifier =
  | DeltaModelModifier
  | PostgresModelModifier
  | IndexModelModifier
  | TypeModelModifier

export interface DeltaModelModifier {
  kind: "DeltaModelModifier";
  value: StringExpression | EnvVarExpression;
}

export interface PostgresModelModifier {
  kind: "PostgresModelModifier";
  value: StringExpression | EnvVarExpression;
}

export interface IndexModelModifier {
  kind: "IndexModelModifier";
  value: StringExpression | EnvVarExpression;
  on: StringExpression;
}

export interface TypeModelModifier {
  kind: "TypeModelModifier";
}

export type Expression =
  | PipeExpression
  | WhereExpression
  | SortExpression
  | OverExpression
  | JoinExpression
  | UnionExpression
  | WithExpression
  | OrExpression
  | AndExpression
  | CmpExpression
  | AddExpression
  | MulExpression
  | DotExpression
  | StringExpression
  | IntegerExpression
  | FloatExpression
  | BooleanExpression
  | EnvVarExpression
  | IdentifierExpression
  | TypeObjectExpression
  | GroupExpression
  | CoalesceExpression

export interface PipeExpression {
  kind: "PipeExpression";
  left: Expression;
  right: Expression;
}

export interface WhereExpression {
  kind: "WhereExpression";
  head?: Expression;
  condition: Expression;
}

export interface SortExpression {
  kind: "SortExpression";
  head?: Expression;
  columns: IdentifierExpression[];
  op: "asc" | "desc";
}

export interface OverExpression {
  kind: "OverExpression";
  head?: Expression;
  column: Expression;
}

export interface JoinExpression {
  kind: "JoinExpression";
  head?: Expression;
  op: "inner" | "left" | "right";
  on?: GroupExpression;
  other: Expression;
}

export interface UnionExpression {
  kind: "UnionExpression";
  head?: Expression;
  other: Expression;
}

export interface WithExpression {
  kind: "WithExpression";
  head?: Expression;
  properties: ObjectProperty[];
}

export type ObjectProperty =
  | AssignObjectProperty
  | AsObjectProperty
  | OpAssignObjectProperty
  | RenameObjectProperty

export interface AssignObjectProperty {
  kind: "AssignObjectProperty";
  name: string;
  value: Expression;
}

export interface AsObjectProperty {
  kind: "AsObjectProperty";
  name: string;
  type: Type;
}

export interface OpAssignObjectProperty {
  kind: "OpAssignObjectProperty";
  name: string;
  op: "??=";
  value: Expression;
}

export interface RenameObjectProperty {
  kind: "RenameObjectProperty";
  name: string;
  value: Expression;
}

export interface CoalesceExpression {
  kind: "CoalesceExpression";
  left: Expression;
  op: "??";
  right: Expression;
}

export interface OrExpression {
  kind: "OrExpression";
  left: Expression;
  op: "or";
  right: Expression;
}

export interface AndExpression {
  kind: "AndExpression";
  left: Expression;
  op: "and";
  right: Expression;
}

export interface CmpExpression {
  kind: "CmpExpression";
  left: Expression;
  op: "==" | "!=" | "<=" | ">=" | "<" | ">";
  right: Expression;
}

export interface AddExpression {
  kind: "AddExpression";
  left: Expression;
  op: "+" | "-";
  right: Expression;
}

export interface MulExpression {
  kind: "MulExpression";
  left: Expression;
  op: "*" | "/" | "%";
  right: Expression;
}

export interface DotExpression {
  kind: "DotExpression";
  left: Expression;
  op: ".";
  right: Expression;
}

export interface StringExpression {
  kind: "StringExpression";
  value: string;
}

export interface IntegerExpression {
  kind: "IntegerExpression";
  value: number;
}

export interface FloatExpression {
  kind: "FloatExpression";
  value: string;
}

export interface BooleanExpression {
  kind: "BooleanExpression";
  value: boolean;
}

export interface EnvVarExpression {
  kind: "EnvVarExpression";
  value: string;
}

export interface IdentifierExpression {
  kind: "IdentifierExpression";
  name: string;
}

export interface TypeObjectExpression {
  kind: "TypeObjectExpression";
  properties: { name: string, type: Type }[];
}

export interface GroupExpression {
  kind: "GroupExpression";
  value: Expression;
}

export function children(e: Expression): Expression[] {
  switch (e.kind) {
    case "StringExpression":
    case "IntegerExpression":
    case "FloatExpression":
    case "BooleanExpression":
    case "EnvVarExpression":
    case "IdentifierExpression":
    case "TypeObjectExpression":
      return [];
    case "GroupExpression":
      return [e.value];
    case "PipeExpression":
    case "OrExpression":
    case "AndExpression":
    case "CmpExpression":
    case "AddExpression":
    case "MulExpression":
    case "DotExpression":
    case "CoalesceExpression":
      return [e.left, e.right]
    case "WhereExpression": return e.head === undefined ? [e.condition] : [e.head, e.condition];
    case "SortExpression": return e.head === undefined ? e.columns : [e.head, ...e.columns];
    case "OverExpression": return e.head === undefined ? [e.column] : [e.head, e.column];
    case "UnionExpression": return e.head === undefined ? [e.other] : [e.head, e.other];
    case "JoinExpression": {
      if (e.head === undefined && e.on === undefined) return [e.other];
      else if (e.head === undefined) return [e.on!, e.other];
      else if (e.on === undefined) return [e.head, e.other];
      else return [e.head, e.on, e.other];
    }
    case "WithExpression": {
      const fromProps = (x: ObjectProperty): Expression[] => {
        switch (x.kind) {
          case "AsObjectProperty": return [];
          case "AssignObjectProperty":
          case "OpAssignObjectProperty":
          case "RenameObjectProperty":
            return [x.value];
        }
      }
      return e.head === undefined ? e.properties.flatMap(fromProps) : [e.head, ...e.properties.flatMap(fromProps)];
    }
  }
}

export function fromChildren(e: Expression, children: Expression[]): Expression {
  switch (e.kind) {
    case "StringExpression":
    case "IntegerExpression":
    case "FloatExpression":
    case "BooleanExpression":
    case "EnvVarExpression":
    case "IdentifierExpression":
    case "TypeObjectExpression":
      return e;
    case "GroupExpression":
      return { ...e, value: children[0] };
    case "PipeExpression":
    case "OrExpression":
    case "AndExpression":
    case "CmpExpression":
    case "AddExpression":
    case "MulExpression":
    case "DotExpression":
    case "CoalesceExpression":
      return { ...e, left: children[0], right: children[1] };
    case "WhereExpression": return e.head === undefined ? { ...e, condition: children[0] } : { ...e, head: children[0], condition: children[1] };
    case "SortExpression": return e.head === undefined ? { ...e, columns: children as IdentifierExpression[] } : { ...e, head: children[0], columns: children.slice(1) as IdentifierExpression[] };
    case "OverExpression": return e.head === undefined ? { ...e, column: children[0] } : { ...e, head: children[0], column: children[1] };
    case "UnionExpression": return e.head === undefined ? { ...e, other: children[0] } : { ...e, head: children[0], other: children[1] };
    case "JoinExpression": {
      if (e.head === undefined && e.on === undefined) return { ...e, other: children[0] };
      else if (e.head === undefined) return { ...e, on: children[0] as GroupExpression, other: children[1] };
      else if (e.on === undefined) return { ...e, head: children[0], other: children[1] };
      else return { ...e, head: children[0], on: children[1] as GroupExpression, other: children[2] };
    }
    case "WithExpression": {
      const makeProps = (x: ObjectProperty, i: number): ObjectProperty => {
        switch (x.kind) {
          case "AsObjectProperty":
            return x;
          case "AssignObjectProperty":
          case "OpAssignObjectProperty":
          case "RenameObjectProperty":
            return { ...x, value: children[i] };
        }
      };
      return e.head === undefined ?
        { ...e, properties: e.properties.map(makeProps) } :
        { ...e, head: children[0], properties: e.properties.map(makeProps) };
    }
  }
}

export function applyToChildren(e: Expression, f: (x: Expression) => Expression): Expression {
  return fromChildren(e, children(e).map(f));
}

export function pull<T, U>(f: (x: T) => [T, U[]], g: (x: T, f: (x: T) => T) => T) {
  return (x: T): [T, U[]] => {
    let l: U[] = [];
    const h = (x: T): T => {
      const [a, b] = f(x);
      l.push(...b);
      return a;
    };
    return [g(x, h), l];
  };
}

const end = <T>(f: (x: T) => string, suffix: string) => (x: T) => f(x) + suffix;
const wrap = (c: string = "\"") => (x: string) => `${c}${x}${c}`;
const quote = wrap("\"");
const cap = (x: string) => x.length === 0 ? "" : `${x[0].toUpperCase()}${x.slice(1)}`;
const line = <T>(f: (x: T) => string) => end(f, '\n');

const formatType = (t: Type): string => {
  switch (t.kind) {
    case "StringType": return "string";
    case "BooleanType": return "bool";
    case "IntegerType": return "int";
    case "FloatType": return "float";
    case "StructType": return "struct ?";
    case "IdentifierType": return t.name;
    case "ArrayType": return `${formatType(t.of)}[]`;
    case "OptionalType": return `${formatType(t.of)}?`;
    case "JsonType": return `${formatType(t.of)} json`;
    case "DateType": return `date${t.fmt ? "" : `"${t.fmt}"`}`;
  }
};

export function format(m: Module): string {
  let indent = '';

  const conditionally = <T>(x: T | undefined, f: (x: T) => string) => x === undefined ? '' : f(x);
  const withIndentation = <T>(x: T, f: (x: T) => string): string => {
    indent += "  ";
    const value = f(x);
    indent = indent.slice(0, -2);
    return value;
  }



  const formatDef = (d: Definition): string => {
    switch (d.kind) {
      case "ModelDefinition":
        return `${d.modifiers.map(line(formatModelMod)).join('')}model ${d.name} = ${formatExpr(d.expression)}`;
    }
  };

  const formatModelMod = (x: ModelModifier): string => {
    switch (x.kind) {
      case "DeltaModelModifier":
        return `delta ${formatExpr(x.value)}`;
      case "IndexModelModifier":
        return `index ${formatExpr(x.value)} on ${formatExpr(x.on)}`;
      case "PostgresModelModifier":
        return `postgres ${formatExpr(x.value)}`;
      case "TypeModelModifier":
        return `type`;
    }
  };

  const formatExpr = (e: Expression): string => {
    switch (e.kind) {
      case "PipeExpression": {
        const fmt = (e: PipeExpression) => `${formatExpr(e.left)}\n${indent}|${formatExpr(e.right)}`;
        if (e.left.kind === "PipeExpression")
          return fmt(e);
        return withIndentation(e, fmt);
      }
      case "WhereExpression":
        return `${conditionally(e.head, formatExpr)} where ${formatExpr(e.condition)}`;
      case "SortExpression":
        return `${conditionally(e.head, formatExpr)} sort ${e.columns.map(formatExpr).join(', ')} ${e.op}`;
      case "OverExpression":
        return `${conditionally(e.head, formatExpr)} over ${formatExpr(e.column)}`;
      case "JoinExpression":
        return `${conditionally(e.head, formatExpr)} ${e.op} join${conditionally(e.on, formatExpr)} ${formatExpr(e.other)}`;
      case "UnionExpression":
        return `${conditionally(e.head, formatExpr)} union ${formatExpr(e.other)}`;
      case "WithExpression":
        return `${conditionally(e.head, formatExpr)} with ${withIndentation(e, e => `{\n${e.properties.map(end(x => {
          switch (x.kind) {
            case "AsObjectProperty":
              return `${indent}${x.name} as ${formatType(x.type)}`;
            case "AssignObjectProperty":
              return `${indent}${x.name} = ${formatExpr(x.value)}`;
            case "OpAssignObjectProperty":
              return `${indent}${x.name} ${x.op} ${formatExpr(x.value)}`;
            case "RenameObjectProperty":
              return `${indent}${x.name} := ${formatExpr(x.value)}`;
          }
        }, ',\n')).join('')}}`)}`;
      case "OrExpression":
      case "AndExpression":
      case "CmpExpression":
      case "AddExpression":
      case "MulExpression":
      case "CoalesceExpression":
        return `${formatExpr(e.left)} ${e.op} ${formatExpr(e.right)}`;
      case "DotExpression":
        return `${formatExpr(e.left)}${e.op}${formatExpr(e.right)}`;
      case "StringExpression":
        return `"${e.value}"`;
      case "IntegerExpression":
      case "FloatExpression":
      case "BooleanExpression":
        return `${e.value}`;
      case "IdentifierExpression":
        return `${e.name}`;
      case "EnvVarExpression":
        return `$"${e.value}"`;
      case "GroupExpression":
        return `(${formatExpr(e.value)})`;
      case "TypeObjectExpression":
        return withIndentation(e, e => `{\n${e.properties.map(end(x => `${indent}${x.name}: ${formatType(x.type)}`, ',\n')).join('')}}`);

    }
  };

  return m.definitions.map(formatDef).join('\n\n');
}

interface Transformation {
  name: string;
  transform: (x: Module) => Module;
}

function applyTransformation(ast: Module, transformation: Transformation): Module {
  const nast = transformation.transform(ast);
  if (process.env.NODE_ENV === "development")
    writeFileSync(`dev/${transformation.name}.relt`, format(nast));
  return nast;
}

class UserError extends Error { }
class InternalError extends Error { }

function assertExpectation(expect: boolean, msg?: string): asserts expect is true {
  if (!expect)
    throw new UserError(msg);
}

function assertInvariant(expect: boolean, msg?: string): asserts expect is true {
  if (!expect)
    throw new InternalError(`Internal error, please report: ${msg ?? "No msg given :("}`);
}

function conditionally<T, K extends keyof T, R>(x: T, key: K, f: (x: T[K] extends infer A | undefined ? A : never) => R) {
  return x[key] === undefined ? undefined : { [key]: f(x[key] as T[K] extends infer A | undefined ? A : never) };
}

function is<K>(kind: K) {
  return <T extends { kind: string }>(x: T): x is T & { kind: K } => x.kind === kind;
}

const identity = {
  name: "identity",
  transform(m) {
    return m;
  }
} satisfies Transformation;

// const resolveNames = {
//   name: "resolveNames",
//   transform(m) {
//     const models = new Set<string>(m.definitions.filter(is("ModelDefinition")).map(x => x.name));
//     let model = '';
//     let alreadyResolved = false;

//     const resolveNamesDef = (d: Definition): Definition => {
//       switch (d.kind) {
//         case "ModelDefinition": {
//           model = d.name;
//           const x = { ...d, expression: resolveNamesExpr(d.expression) };
//           model = '';
//           return x;
//         }
//       }
//     };

//     const resolveNamesExpr = (e: Expression): Expression => {
//       switch (e.kind) {
//         case "StringExpression":
//         case "IntegerExpression":
//         case "FloatExpression":
//         case "BooleanExpression":
//         case "EnvVarExpression":
//         case "TypeObjectExpression":
//           return e;

//         case "GroupExpression":
//           return { ...e, value: resolveNamesExpr(e.value) };

//         case "WithExpression":
//           return { kind: "WithExpression", ...conditionally(e, 'head', resolveNamesExpr), properties: e.properties.map(x => ({ name: x.name, value: resolveNamesExpr(x.value) })) };

//         case "OrExpression":
//         case "AndExpression":
//         case "CmpExpression":
//         case "AddExpression":
//         case "MulExpression":
//         case "PipeExpression":
//           return { ...e, left: resolveNamesExpr(e.left), right: resolveNamesExpr(e.right) };

//         case "WhereExpression":
//           return { ...e, condition: resolveNamesExpr(e.condition), ...conditionally(e, 'head', resolveNamesExpr) };
//         case "SortExpression":
//           return { ...e, columns: e.columns.map(resolveNamesExpr) as IdentifierExpression[], ...conditionally(e, 'head', resolveNamesExpr) };
//         case "OverExpression":
//           return { ...e, column: resolveNamesExpr(e.column), ...conditionally(e, 'head', resolveNamesExpr) };
//         case "JoinExpression":
//           return { ...e, other: resolveNamesExpr(e.other), ...conditionally(e, 'head', resolveNamesExpr), ...conditionally(e, 'on', resolveNamesExpr) };
//         case "UnionExpression":
//           return { ...e, other: resolveNamesExpr(e.other), ...conditionally(e, 'head', resolveNamesExpr) };

//         case "DotExpression": {
//           alreadyResolved = true;
//           const x = { ...e, left: resolveNamesExpr(e.left), right: resolveNamesExpr(e.right) };
//           alreadyResolved = false;
//           return x;
//         }

//         case "IdentifierExpression": {
//           if (models.has(e.name) || alreadyResolved) return e;
//           return { kind: "DotExpression", left: { kind: "IdentifierExpression", name: model }, op: ".", right: e };
//         }
//       }
//     };

//     return { kind: "Module", definitions: m.definitions.map(resolveNamesDef) };
//   }
// } satisfies Transformation;

const noPipes = {
  name: "noPipes",
  transform(m) {
    let c = 0;

    const def = (x: Definition): Definition[] => {
      switch (x.kind) {
        case "ModelDefinition": {
          const [e, d] = expr(x.expression);
          return [...d, { ...x, expression: e }];
        }
        default:
          return [x];
      }
    };

    const makeHead = (l: Expression, r: Expression): Expression => {
      switch (r.kind) {
        case "WhereExpression":
        case "UnionExpression":
        case "OverExpression":
        case "JoinExpression":
        case "SortExpression":
        case "WithExpression":
          return { ...r, head: l };
        default:
          assertExpectation(false, `Cannot unpipe ${l.kind}`); /* hint for typescript */ throw '';
      }
    };

    const expr = (x: Expression): [Expression, ModelDefinition[]] => {
      switch (x.kind) {
        case "PipeExpression": {
          // we need to make a model!
          if (x.left.kind === "PipeExpression") {
            const [e, ds] = expr(x.left);

            const name = `RELT_${c++}`;

            const d = {
              kind: "ModelDefinition",
              modifiers: [],
              name,
              expression: e,
            } satisfies ModelDefinition;

            return [makeHead({ kind: "IdentifierExpression", name }, x.right), [...ds, d]];
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

    return { kind: "Module", definitions: m.definitions.flatMap(def) };
  },
} satisfies Transformation;

class DAG {
  private feeds = new Map<string, string[]>();
  private consumes = new Map<string, string[]>();

  addVertex(v: string) {
    if (this.feeds.has(v) || this.consumes.has(v))
      assertInvariant(false, `${v} is already in the dag!`);
    this.feeds.set(v, []);
    this.consumes.set(v, []);
  }

  addEdge(start: string, end: string) {
    if (!this.feeds.has(start))
      assertInvariant(false, `Unknown vertex: ${start}`);
    if (!this.consumes.has(end))
      assertInvariant(false, `Unknown vertex: ${end}`);
    this.feeds.get(start)!.push(end);
    this.consumes.get(end)!.push(start);
  }

  sort(): string[] {
    return [];
  }

  feedsInto(x: string): string[] {
    if (!this.feeds.has(x))
      assertInvariant(false, `unknown vertex: ${x}`);
    return this.feeds.get(x)!;
  }

  consumeFrom(x: string): string[] {
    if (!this.consumes.has(x))
      assertInvariant(false, `unknown vertex: ${x}`);
    return this.consumes.get(x)!;
  }
}

const deps = new DAG();

const dag = {
  name: "dag",
  transform(x) {
    const def = (x: Definition): Definition => {
      switch (x.kind) {
        case "ModelDefinition": {
          deps.addVertex(x.name);
          return x;
        }
      }
    };
    return { kind: "Module", definitions: x.definitions.map(def) };
  },
} satisfies Transformation;

function typeEquals(l: Type, r: Type): boolean {
  switch (l.kind) {
    case "BooleanType": return r.kind === "BooleanType";
    case "FloatType": return r.kind === "FloatType";
    case "IntegerType": return r.kind === "IntegerType";
    case "StringType": return r.kind === "StringType";
    case "IdentifierType": return r.kind === "IdentifierType" && l.name === r.name;
    case "ArrayType": return r.kind === "ArrayType" && typeEquals(l.of, r.of);
    case "OptionalType": return r.kind === "OptionalType" && typeEquals(l.of, r.of);
    case "DateType": return r.kind === "DateType";
    case "JsonType": return typeEquals(r, l.of); // TODO think about this and check if it is trying to be too smart
    case "StructType":
      switch (r.kind) {
        case "StructType": {
          if (l.properties.length !== r.properties.length) return false;
          const lP = new Map(l.properties.map(x => [x.name, x.type]));
          const rP = new Map(r.properties.map(x => [x.name, x.type]));
          if (l.properties.some(x => !rP.has(x.name))) return false;
          if (r.properties.some(x => !lP.has(x.name))) return false;
          return l.properties.every(x => typeEquals(lP.get(x.name)!, rP.get(x.name)!));
        }
        default: return false;
      }
  }
}

function mergeStructs(l: StructType, r: StructType, options?: { allowOverride: boolean }): StructType {
  const { allowOverride } = options ?? { allowOverride: false };
  const properties: StructType['properties'] = [...l.properties];
  for (const p of r.properties) {
    if (!allowOverride)
      assertExpectation(properties.findIndex(x => x.name === p.name) === -1, `${p.name} is already in struct`);
    properties.push(p);
  }
  return { kind: "StructType", properties };
}

const types = {
  bool: { kind: "BooleanType" } satisfies BooleanType,
  int: { kind: "IntegerType" } satisfies IntegerType,
  float: { kind: "FloatType" } satisfies FloatType,
  date: { kind: "DateType" } satisfies DateType,
  string: { kind: "StringType" } satisfies StringType,
  opt: (of: Type) => ({ kind: "OptionalType", of }) satisfies OptionalType,
};

function overload(args: Type[], params: Type[], res: Type): Type | undefined {
  if (args.length !== params.length) return undefined;
  if (args.every((_, i) => typeEquals(args[i], params[i])))
    return res;
  return undefined;
}

function opTypeCheck(l: Type, op: string, r: Type): Type | undefined {
  switch (op) {
    case "??": {
      if (l.kind !== "OptionalType") return undefined;
      if (!typeEquals(l.of, r)) return undefined;
      return l.of;
    }
    case "or": return (undefined
      || overload([l, r], [types.bool, types.int], types.bool)
      || overload([l, r], [types.opt(types.bool), types.opt(types.bool)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.bool), types.bool], types.opt(types.bool))
      || overload([l, r], [types.bool, types.opt(types.bool)], types.opt(types.bool))
    );
    case "and": return (undefined
      || overload([l, r], [types.bool, types.int], types.bool)
      || overload([l, r], [types.opt(types.bool), types.opt(types.bool)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.bool), types.bool], types.opt(types.bool))
      || overload([l, r], [types.bool, types.opt(types.bool)], types.opt(types.bool))
    );
    case "==": return (undefined
      || overload([l, r], [r, l], types.bool)
      || overload([l, r], [types.opt(r), l], types.opt(types.bool))
      || overload([l, r], [r, types.opt(l)], types.opt(types.bool))
      || overload([l, r], [types.opt(r), types.opt(l)], types.opt(types.bool))
    );
    case "!=": return (undefined
      || overload([l, r], [r, l], types.bool)
      || overload([l, r], [types.opt(r), l], types.opt(types.bool))
      || overload([l, r], [r, types.opt(l)], types.opt(types.bool))
      || overload([l, r], [types.opt(r), types.opt(l)], types.opt(types.bool))
    );
    case "<=": return (undefined
      || overload([l, r], [types.int, types.int], types.bool)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.bool))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.bool))

      || overload([l, r], [types.float, types.float], types.bool)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.bool))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.bool))

      || overload([l, r], [types.date, types.date], types.bool)
      || overload([l, r], [types.opt(types.date), types.opt(types.date)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.date), types.date], types.opt(types.bool))
      || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool))
    );
    case ">=": return (undefined
      || overload([l, r], [types.int, types.int], types.bool)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.bool))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.bool))

      || overload([l, r], [types.float, types.float], types.bool)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.bool))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.bool))

      || overload([l, r], [types.date, types.date], types.bool)
      || overload([l, r], [types.opt(types.date), types.opt(types.date)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.date), types.date], types.opt(types.bool))
      || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool))
    );
    case "<": return (undefined
      || overload([l, r], [types.int, types.int], types.bool)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.bool))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.bool))

      || overload([l, r], [types.float, types.float], types.bool)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.bool))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.bool))

      || overload([l, r], [types.date, types.date], types.bool)
      || overload([l, r], [types.opt(types.date), types.opt(types.date)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.date), types.date], types.opt(types.bool))
      || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool))
    );
    case ">": return (undefined
      || overload([l, r], [types.int, types.int], types.bool)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.bool))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.bool))

      || overload([l, r], [types.float, types.float], types.bool)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.bool))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.bool))

      || overload([l, r], [types.date, types.date], types.bool)
      || overload([l, r], [types.opt(types.date), types.opt(types.date)], types.opt(types.bool))
      || overload([l, r], [types.opt(types.date), types.date], types.opt(types.bool))
      || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool))
    );
    case "+": return (undefined
      || overload([l, r], [types.int, types.int], types.int)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))

      || overload([l, r], [types.float, types.float], types.float)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float))

      || overload([l, r], [types.date, types.date], types.date)
      || overload([l, r], [types.opt(types.date), types.opt(types.date)], types.opt(types.date))
      || overload([l, r], [types.opt(types.date), types.date], types.opt(types.date))
      || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.date))

      || overload([l, r], [types.string, types.string], types.string)
      || overload([l, r], [types.opt(types.string), types.opt(types.string)], types.opt(types.string))
      || overload([l, r], [types.opt(types.string), types.string], types.opt(types.string))
      || overload([l, r], [types.string, types.opt(types.string)], types.opt(types.string))
    );
    case "-": return (undefined
      || overload([l, r], [types.int, types.int], types.int)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))

      || overload([l, r], [types.float, types.float], types.float)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float))

      || overload([l, r], [types.date, types.date], types.date)
      || overload([l, r], [types.opt(types.date), types.opt(types.date)], types.opt(types.date))
      || overload([l, r], [types.opt(types.date), types.date], types.opt(types.date))
      || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.date))
    );
    case "*": return (undefined
      || overload([l, r], [types.int, types.int], types.int)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))

      || overload([l, r], [types.float, types.float], types.float)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float))
    );
    case "/": return (undefined
      || overload([l, r], [types.int, types.int], types.int)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))

      || overload([l, r], [types.float, types.float], types.float)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float))
    );
    case "%": return (undefined
      || overload([l, r], [types.int, types.int], types.int)
      || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
      || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
      || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))

      || overload([l, r], [types.float, types.float], types.float)
      || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
      || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
      || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float))
    );
  }
}

class TypeChecker {
  private models = new Map<string, StructType>();

  typeCheck(x: ModelDefinition): StructType {
    if (this.models.has(x.name))
      return this.models.get(x.name)!;

    const t = this.typeCheckExpr(x.expression);

    assertExpectation(is('StructType')(t), "");

    this.models.set(x.name, t as StructType);
    this.ctx.clear();

    return t as StructType;
  }

  private ctx = new Map<string, Type>();
  typeCheckExpr(x: Expression): Type {
    switch (x.kind) {
      case "BooleanExpression": return types.bool;
      case "FloatExpression": return types.float;
      case "StringExpression": return types.string;
      case "EnvVarExpression": return types.string;
      case "IntegerExpression": return types.int;
      case "GroupExpression": return this.typeCheckExpr(x.value);
      case "IdentifierExpression": {
        if (this.models.has(x.name))
          return this.models.get(x.name)!;
        assertExpectation(this.ctx.has(x.name), `Unknown variable ${x.name}`);
        return this.ctx.get(x.name)!;
      }
      case "TypeObjectExpression":
        return { kind: "StructType", properties: x.properties };
      case "DotExpression": {
        const l = this.typeCheckExpr(x.left);
        assertExpectation(is('StructType')(l), `Left side of a dot needs to be a struct type`);
        const oldCtx = new Map(this.ctx);
        this.ctx = new Map((l as StructType).properties.map(x => [x.name, x.type]));
        const r = this.typeCheckExpr(x.right);
        this.ctx = oldCtx;
        return r;
      }

      case "OrExpression":
      case "AndExpression":
      case "AddExpression":
      case "CmpExpression":
      case "MulExpression":
      case "CoalesceExpression": {
        const l = this.typeCheckExpr(x.left);
        const r = this.typeCheckExpr(x.right);
        const t = opTypeCheck(l, x.op, r);
        assertExpectation(t !== undefined, `Bad ${x.op} with (${l.kind}, ${r.kind})`);
        return t!;
      }

      case "PipeExpression": assertInvariant(false, `Pipes should be removed`); throw '';

      case "JoinExpression": {
        assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
        const l = this.typeCheckExpr(x.head!);
        assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
        const r = this.typeCheckExpr(x.other);
        assertExpectation(is('StructType')(r), `Right side of join needs to be a struct type`);
        return mergeStructs(l as StructType, r as StructType);
      }
      case "SortExpression": {
        assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
        const l = this.typeCheckExpr(x.head!);
        assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
        const oldCtx = new Map(this.ctx);
        this.ctx = new Map((l as StructType).properties.map(x => [x.name, x.type]));
        x.columns.forEach(x => this.typeCheckExpr(x));
        this.ctx = oldCtx;
        return l;
      }
      case "WhereExpression": {
        assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
        const l = this.typeCheckExpr(x.head!);
        assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
        const oldCtx = new Map(this.ctx);
        this.ctx = new Map((l as StructType).properties.map(x => [x.name, x.type]));
        const c = this.typeCheckExpr(x.condition);
        assertExpectation(typeEquals(c, types.bool), `Where condition needs to be a bool got ${formatType(c)}`);
        this.ctx = oldCtx;
        return l;
      }
      case "UnionExpression": {
        assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
        const l = this.typeCheckExpr(x.head!);
        assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
        const r = this.typeCheckExpr(x.other);
        assertExpectation(is('StructType')(r), `Right side of join needs to be a struct type`);
        assertExpectation(typeEquals(l, r), `Cannot union non equal struct types`);
        return l;
      }
      case "WithExpression":
        assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
        const l = this.typeCheckExpr(x.head!);
        assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
        const properties = x.properties.map(x => ({ name: x.name, type: this.typeCheckExpr(x.value) }));
        return mergeStructs(l as StructType, { kind: "StructType", properties }, { allowOverride: true });
      case "OverExpression":
        throw `TODO`;
    }
  }
}

const tc = new TypeChecker();

export interface ScalaCaseClass {
  name: string;
  properties: { name: string, type: ScalaType }[];
}

export type ScalaType =
  | { kind: "ScalaDoubleType" }
  | { kind: "ScalaIntType" }
  | { kind: "ScalaBooleanType" }
  | { kind: "ScalaStringType" }
  | { kind: "ScalaIdentifierType", name: string }
  | { kind: "ScalaDotType", left: ScalaType, right: ScalaType }
  | { kind: "ScalaOfType", type: ScalaType, of: ScalaType }
  | { kind: "ScalaDateType" }

export interface SparkHandler {
  name: string;
  typeName: string;
  combine: string;
  create?: SparkTableFunction;
  storage?: SparkStorage;
  plans: SparkPlans;
  consumes: string[];
  feeds: string[];
  hasCache: boolean;
}

export interface SparkStorage {
  name: string;
  args: string[];
}

export interface SparkPlan {
  args: string[];
  typeArgs: string[];
}

export interface SparkPlans {
  invalidatePlan?: SparkPlan;
  storePlan?: SparkPlan;
  refreshPlan?: SparkPlan;
  fetchPlan?: SparkPlan;
  derivePlan?: SparkPlan;
}

export type SparkTableFunction =
  | { kind: "SparkJoinFunction", lIdx: number, rIdx: number, on: SparkExpression, type: "inner" | "left" | "right", typeName: string }
  | { kind: "SparkUnionFunction", lIdx: number, rIdx: number, }
  | { kind: "SparkWhereFunction", idx: number, cond: SparkExpression }
  | { kind: "SparkSortFunction", idx: number, columns: string[], type: "asc" | "desc" }
  | { kind: "SparkWithColumnFunction", idx: number, properties: { name: string, value: SparkExpression, rename: boolean }[], typeName: string }

export type SparkExpression =
  | { kind: "SparkLiteralExpression", value: string }
  | { kind: "SparkColumnExpression", value: string }
  | { kind: "SparkGroupExpression", value: SparkExpression }
  | { kind: "SparkBinaryExpression", left: SparkExpression, op: string, right: SparkExpression }
  | { kind: "SparkAppExpression", func: string, args: SparkExpression[] }

function toScalaCaseClass(x: ModelDefinition): ScalaCaseClass {
  const t = tc.typeCheck(x);
  return { name: x.name, properties: t.properties.map(x => ({ name: x.name, type: toScalaType(x.type) })) };
}

function toScalaType(x: Type): ScalaType {
  switch (x.kind) {
    case "BooleanType": return { kind: "ScalaBooleanType" };
    case "FloatType": return { kind: "ScalaDoubleType" };
    case "IntegerType": return { kind: "ScalaIntType" };
    case "StringType": return { kind: "ScalaStringType" };
    case "StructType": assertInvariant(false, 'Should not be able to convert a struct type to scala'); throw '';
    case "IdentifierType": return { kind: "ScalaDotType", left: { kind: "ScalaIdentifierType", name: "Types" }, right: { kind: "ScalaIdentifierType", name: x.name } };
    case "JsonType": return toScalaType(x.of);
    case "DateType": return { kind: "ScalaDateType" };
    case "ArrayType": return { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Array" }, of: toScalaType(x.of) };
    case "OptionalType": return { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Option" }, of: toScalaType(x.of) };
  }
}

function createStorage(x: ModelDefinition): SparkStorage | undefined {
  let storage: SparkStorage | undefined = undefined;

  for (const m of x.modifiers) {
    switch (m.kind) {
      case "DeltaModelModifier": {
        assertExpectation(storage === undefined, `Cannot have multiple storage modifiers`);
        const name = `DeltaFileStorage`;
        switch (m.value.kind) {
          case "StringExpression":
            storage = {
              name,
              args: [`"${m.value.value}"`],
            };
            break;
          case "EnvVarExpression":
            storage = {
              name,
              args: [`"${m.value.value}"`], // TODO emit scala code which reads the env
            };
            break;
        }
        break;
      }
      case "PostgresModelModifier": {
        assertExpectation(storage === undefined, `Cannot have multiple storage modifiers`);
        const name = `PostgresTableStorage`;
        switch (m.value.kind) {
          case "StringExpression":
            storage = {
              name,
              args: [`"${m.value.value}"`],
            };
            break;
          case "EnvVarExpression":
            storage = {
              name,
              args: [`"${m.value.value}"`], // TODO emit scala code which reads the env
            };
            break;
        }
        break;
      }
      case "IndexModelModifier": {
        assertExpectation(storage === undefined, `Cannot have multiple storage modifiers`);
        const name = `IndexerStorage`;
        switch (m.value.kind) {
          case "StringExpression":
            storage = {
              name,
              args: [`"${m.value.value}"`, `"${m.on.value}"`],
            };
            break;
          case "EnvVarExpression":
            storage = {
              name,
              args: [`"${m.value.value}"`, `"${m.on.value}"`], // TODO emit scala code which reads the env
            };
            break;
        }
        break;
      }
      case "TypeModelModifier":
        break;
    }
  }

  return storage;
}

function isSourceType(x: ModelDefinition): boolean {
  if (x.modifiers.some(x => x.kind === "TypeModelModifier"))
    return false;

  const expr = (x: Expression): boolean => {
    switch (x.kind) {
      case "TypeObjectExpression": return true;
      default: return children(x).some(expr);
    }
  }
  return expr(x.expression);
}

function makePlans(x: ModelDefinition, isSource: boolean, hasCache: boolean): SparkPlans {
  const mods = new Set(x.modifiers.map(x => x.kind));

  if (mods.has('TypeModelModifier')) return {};

  const hasInvalidate = !isSource && hasCache && !mods.has('IndexModelModifier');
  const hasStore = hasCache;
  const hasFetch = isSource || hasCache;
  const hasRefresh = isSource;
  const hasDerive = !isSource;

  return {
    ...!hasInvalidate ? {} : {
      invalidatePlan: { args: ["storage"], typeArgs: ['DeltaTypes', 'Types'] },
    },
    ...!hasStore ? {} : {
      storePlan: { args: ["storage", "create", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
    },
    ...!hasFetch ? {} : {
      fetchPlan: { args: ["storage", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
    },
    ...!hasRefresh ? {} : {
      refreshPlan: { args: ["storage", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
    },
    ...!hasDerive ? {} : {
      derivePlan: { args: ["create", "combine"], typeArgs: ['DeltaTypes'] }
    },
  };
}

function makeCreateFunction(x: ModelDefinition, indices: Map<string, number>, mapping: Map<string, string>): SparkTableFunction {
  switch (x.expression.kind) {
    case "JoinExpression":
      return {
        kind: "SparkJoinFunction",
        lIdx: indices.get((x.expression.head as IdentifierExpression).name)!,
        on: x.expression.on === undefined ? (() => { assertInvariant(false, `Cannot make join without on condition`); throw ''; })() : makeSparkExpression(x.expression.on),
        rIdx: indices.get((x.expression.other as IdentifierExpression).name)!,
        type: x.expression.op,
        typeName: mapping.get(x.name)!,
      };
    case "SortExpression":
      return {
        kind: "SparkSortFunction",
        idx: indices.get((x.expression.head as IdentifierExpression).name)!,
        columns: x.expression.columns.map(x => x.name),
        type: "asc"
      };
    case "UnionExpression":
      return {
        kind: "SparkUnionFunction",
        lIdx: indices.get((x.expression.head as IdentifierExpression).name)!,
        rIdx: indices.get((x.expression.other as IdentifierExpression).name)!,
      };
    case "WhereExpression":
      return {
        kind: "SparkWhereFunction",
        idx: indices.get((x.expression.head as IdentifierExpression).name)!,
        cond: makeSparkExpression(x.expression.condition),
      };
    case "WithExpression":
      return {
        kind: "SparkWithColumnFunction",
        idx: indices.get((x.expression.head as IdentifierExpression).name)!,
        properties: x.expression.properties.map(x => {
          switch (x.kind) {
            case "AsObjectProperty"
          }
        }),
        typeName: mapping.get(x.name)!,
      };
    case "OverExpression":
      assertInvariant(false, `TODO`); throw '';
    default:
      assertInvariant(false, `Cannot create a creation function for model which does not have a table expression`); throw '';
  }
}

function makeSparkExpression(x: Expression): SparkExpression {
  switch (x.kind) {
    case "BooleanExpression": return { kind: "SparkLiteralExpression", value: `${x.value}` };
    case "EnvVarExpression": return { kind: "SparkLiteralExpression", value: `"${x.value}"` }; // TODO get env in scala
    case "IntegerExpression": return { kind: "SparkLiteralExpression", value: `${x.value}` };
    case "StringExpression": return { kind: "SparkLiteralExpression", value: `"${x.value}"` };
    case "FloatExpression": return { kind: "SparkLiteralExpression", value: `${x.value}` };
    case "GroupExpression": return { kind: "SparkGroupExpression", value: makeSparkExpression(x.value) };
    case "IdentifierExpression": return { kind: "SparkColumnExpression", value: x.name };
    case "AddExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: "and", right: makeSparkExpression(x.right) };
    case "AndExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: "and", right: makeSparkExpression(x.right) };
    case "CmpExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: { "==": "===", "!=": "!=", "<=": "<=", ">=": ">=", "<": "<", ">": ">" }[x.op], right: makeSparkExpression(x.right) };
    case "MulExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: x.op, right: makeSparkExpression(x.right) };
    case "OrExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: x.op, right: makeSparkExpression(x.right) };
    case "CoalesceExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: ".", right: { kind: "SparkAppExpression", func: "getOrElse", args: [makeSparkExpression(x.right)] } };

    case "OverExpression":
    case "PipeExpression":
    case "SortExpression":
    case "TypeObjectExpression":
    case "UnionExpression":
    case "WhereExpression":
    case "WithExpression":
    case "JoinExpression":
    case "DotExpression":
      assertInvariant(false, `Cannot convert ${x.kind} to spark expression`); throw '';
  }
}

function toSparkHandler(x: ModelDefinition, indices: Map<string, number>, mapping: Map<string, string>): SparkHandler {
  const idx = indices.get(x.name)!;
  const count = mapping.size;
  const storage = createStorage(x);
  const isSource = isSourceType(x);
  const hasCache = storage !== undefined && !isSource;
  const typeName = mapping.get(x.name)!;
  return {
    name: x.name,
    typeName,
    combine: `(${Array.from({ length: count }).map((_, i) => i === idx ? "ds" : `dss._${i + 1}`).join(', ')})`,
    feeds: deps.feedsInto(x.name),
    consumes: deps.consumeFrom(x.name),
    hasCache,
    plans: makePlans(x, isSource, hasCache),
    storage,
    create: isSource ? undefined : makeCreateFunction(x, indices, mapping),
  };
}

function addDeltaProperty(x: ScalaCaseClass): ScalaCaseClass {
  return {
    ...x,
    properties: [
      ...x.properties,
      {
        name: "__delta_state_kind",
        type: {
          kind: "ScalaDotType",
          left: { kind: "ScalaIdentifierType", name: "DeltaState" },
          right: { kind: "ScalaIdentifierType", name: "Kind" }
        }
      },
    ]
  };
}

function formatScalaType(x: ScalaType): string {
  switch (x.kind) {
    case "ScalaBooleanType": return "Boolean";
    case "ScalaDotType": return `${formatScalaType(x.left)}.${formatScalaType(x.right)}`;
    case "ScalaDoubleType": return "Double";
    case "ScalaIdentifierType": return x.name;
    case "ScalaIntType": return "Int";
    case "ScalaStringType": return "String";
    case "ScalaOfType": return `${formatScalaType(x.type)}[${formatScalaType(x.of)}]`;
    case "ScalaDateType": return `Date`;
  }
}

function formatScalaCaseClass(x: ScalaCaseClass): string {
  return `case class ${x.name} (\n${x.properties.map(end(x => `  ${x.name}: ${formatScalaType(x.type)}`, ',\n')).join('')})`;
}

function formatSparkHandler(x: SparkHandler): string {
  return `object ${x.name} extends Handler[DeltaTypes.Datasets] {
    def combine(dss: DeltaTypes.Datasets, ds: Dataset[DeltaTypes.${x.typeName}]): DeltaTypes.Datasets = {
      return ${x.combine}
    }

    ${x.create === undefined ? "" :
      `private def create(spark: SparkSession, dss: DeltaTypes.Datasets): Dataset[DeltaTypes.${x.typeName}] = {
        import spark.implicits._
      return ${formatSparkTableFunction(x.create)}
    }`}

    ${x.storage === undefined ? "" :
      `private val storage = new ${x.storage.name}[Types.${x.typeName}](${x.storage.args.join(', ')})`
    }

    ${Object.entries(x.plans).map(([k, v]: [string, SparkPlan]) => `private val ${k} = new ${cap(k)}[DeltaTypes.Datasets, ${v.typeArgs.map(a => `${a}.${x.typeName}`)}](${v.args.map(x => `this.${x}`).join(',')})\n`).join('')}

    val name = "${x.name}"
    val consumes = Array[String](${x.consumes.map(quote).join(', ')})
    val feeds = Array[String](${x.consumes.map(quote).join(', ')})
    val hasCache = ${x.hasCache}
    val plans = Map(
      ${Object.keys(x.plans).map(k => `${quote(cap(k))} -> this.${k},\n`).join('')}
    )
  }`;
}

function formatSparkTableFunction(x: SparkTableFunction): string {
  switch (x.kind) {
    case "SparkJoinFunction": return `dss._${x.lIdx + 1}.join(dss._${x.rIdx + 1}, ${formatSparkExpression(x.on)}, "${x.type}").as[DeltaTypes.${x.typeName}]`; // try without as
    case "SparkSortFunction": return `dss._${x.idx + 1}.orderBy(${x.columns.map(c => `col("${c}").${x.type}`)})`; // try without as
    case "SparkUnionFunction": return `dss._${x.lIdx + 1}.union(dss._${x.rIdx + 1})`; // try without as
    case "SparkWhereFunction": return `dss._${x.idx + 1}.where(${formatSparkExpression(x.cond)})`; // try without as  
    case "SparkWithColumnFunction": return `dss._${x.idx + 1}${x.properties.map(x => `.withColumn("${x.name}", ${formatSparkExpression(x.value)})`)}.as[DeltaTypes.${x.typeName}]`;
  }
}

function formatSparkExpression(x: SparkExpression): string {
  switch (x.kind) {
    case "SparkBinaryExpression": return `(${formatSparkExpression(x.left)} ${x.op} ${formatSparkExpression(x.right)})`
    case "SparkColumnExpression": return `col("${x.value}")`;
    case "SparkLiteralExpression": return `lit(${x.value})`;
    case "SparkGroupExpression": return `(${formatSparkExpression(x.value)})`;
    case "SparkAppExpression": return `${x.func}(${x.args.map(formatSparkExpression).join(', ')})`;
  }
}

function justSparkModels(x: ModelDefinition[]): ModelDefinition[] {
  return x.filter(x => x.modifiers.findIndex(x => x.kind === "TypeModelModifier") === -1);;
}

function makeScalaCaseClasses(models: ModelDefinition[]): [ScalaCaseClass[], ScalaCaseClass[], Map<string, string>] {
  const classes = new Map<string, number>();
  const mapping = new Map<string, string>();
  let skip = false;
  for (const [i, model] of models.entries()) {
    const modelType = tc.typeCheck(model);
    skip = false;
    for (const [k, v] of classes) {
      const type = tc.typeCheck(models[v]);
      if (typeEquals(modelType, type)) {
        mapping.set(model.name, k);
        skip = true;
        break;
      }
    }
    if (!skip) {
      classes.set(model.name, i);
      // model is a spark model, aka not a type model
      if (justSparkModels([model]).length === 1)
        mapping.set(model.name, model.name);
    }
  }

  const uniqueModels = [...classes.values()].map(x => models[x]);
  const sparkModels = justSparkModels(uniqueModels);

  return [
    sparkModels.map(toScalaCaseClass),
    uniqueModels.map(toScalaCaseClass),
    mapping
  ];
}

export function emit(m: Module): string {
  const models = m.definitions.filter(is("ModelDefinition"));
  const sparkModels = justSparkModels(models);
  const indices = new Map(sparkModels.map((x, i) => [x.name, i]));


  const [sparkTypes, allTypes, mapping] = makeScalaCaseClasses(models);
  const deltaTypes = sparkTypes.map(addDeltaProperty);
  const handlers = sparkModels.map(x => toSparkHandler(x, indices, mapping));

  return scalaTemplate({
    packageName: "Example",
    names: [...mapping.values()],
    allNames: sparkModels.map(x => x.name),
    types: allTypes.map(formatScalaCaseClass),
    deltaTypes: deltaTypes.map(formatScalaCaseClass),
    handlers: handlers.map(formatSparkHandler),
  });
}

class Ast {
  constructor(private module: Module) {
  }

  apply(t: Transformation): Ast {
    this.module = applyTransformation(this.module, t);
    return this;
  }

  emit() {
    return emit(this.module);
  }
}

async function main() {
  const fileContent = (await readFile('main.relt')).toString();
  let ast = new Ast(parser.parse(fileContent) as Module);

  // console.log(inspect(ast, false, null, true));

  const scala = ast
    .apply(identity)
    .apply(noPipes)
    .apply(dag)
    .emit();

  await writeFile('target/src/main/scala/out.scala', scala);
}

main();
