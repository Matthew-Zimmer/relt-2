import { match } from "ts-pattern";
import { assertDefined, throws } from "../../../errors";
import { ReltBooleanExpression, ReltEnvVarExpression, ReltExpression, ReltExternalModelModifier, ReltFloatExpression, ReltIntegerExpression, ReltModelDefinition, ReltStringExpression, ReltType } from "../types";

export type Instruction =
  | RefreshInstruction
  | FetchInstruction
  | DeriveInstruction
  | StoreInstruction

export interface RefreshInstruction {
  kind: "RefreshInstruction";
  name: string;
  className: string;
  storageClassName: string;
  deltaClassName: string;
}

export interface FetchInstruction {
  kind: "FetchInstruction";
  name: string;
  className: string;
  storageClassName: string;
  deltaClassName: string;
}

export interface DeriveInstruction {
  kind: "DeriveInstruction";
  name: string;
  className: string;
  steps: DeriveStep[];
}

export interface StoreInstruction {
  kind: "StoreInstruction";
  name: string;
  className: string;
  storageClassName: string;
}


export type DeriveStep =
  | UseStep
  | JoinStep
  | WithStep
  | AsStep
  | RetStep
  | WhereStep
  | SortStep
  | UnionStep
  | RenameStep
  | CallStep

export interface UseStep {
  kind: "UseStep";
  dssIdx: number;
  dest: number;
}

export interface JoinStep {
  kind: "JoinStep";
  left: number;
  right: number;
  dest: number;
  type: "inner" | "left" | "right";
  condition: ColumnExpression;
}

export interface UnionStep {
  kind: "UnionStep";
  left: number;
  right: number;
  dest: number;
}

export interface WhereStep {
  kind: "WhereStep";
  ds: number;
  dest: number;
  condition: ColumnExpression;
}

export interface SortStep {
  kind: "SortStep";
  ds: number;
  dest: number;
  mode: "asc" | "desc";
  columns: ColumnExpression[];
}

export interface WithStep {
  kind: "WithStep";
  ds: number;
  dest: number;
  name: string;
  value: ColumnExpression;
}

export interface RenameStep {
  kind: "RenameStep";
  ds: number;
  dest: number;
  name: string;
  value: ColumnExpression;
}

export interface AsStep {
  kind: "AsStep";
  ds: number;
  dest: number;
  type: ReltType;
}

export interface RetStep {
  kind: "RetStep";
  ds: number;
}

export interface CallStep {
  kind: "CallStep";
  dest: number;
  name: string;
  args: (string | number)[];
}

export type ColumnExpression =
  | ColumnBinaryOpExpression
  | ColumnLiteralExpression
  | ColumnColExpression
  | ColumnAppExpression
  | ColumnWhenExpression
  | ColumnGroupExpression
  | ColumnIdentifierExpression

export interface ColumnBinaryOpExpression {
  kind: "ColumnBinaryOpExpression";
  left: ColumnExpression;
  op: string;
  right: ColumnExpression;
}

export interface ColumnLiteralExpression {
  kind: "ColumnLiteralExpression";
  value: ReltIntegerExpression | ReltBooleanExpression | ReltStringExpression | ReltFloatExpression;
}

export interface ColumnColExpression {
  kind: "ColumnColExpression";
  name: string;
}

export interface ColumnAppExpression {
  kind: "ColumnAppExpression";
  col: ColumnExpression;
  func: "cast" | "isNull" | "isNotNull"
  args: {}[];
}

export interface ColumnWhenExpression {
  kind: "ColumnWhenExpression";
  condition: ColumnExpression;
  ifTrue: ColumnExpression;
  ifFalse: ColumnExpression;
}

export interface ColumnGroupExpression {
  kind: "ColumnGroupExpression";
  value: ColumnExpression;
}

export interface ColumnIdentifierExpression {
  kind: "ColumnIdentifierExpression";
  name: string;
}

export function instructionsFor(model: ReltModelDefinition, indices: Record<string, number>): Instruction[] {
  if (isTypeModel(model)) return [];

  const isSource = isSourceModel(model);
  const isStored = isStoredModel(model);
  const isExternal = isExternalModel(model);

  const hasRefreshInstruction = isSource;
  const hasFetchInstruction = isStored;
  const hasDeriveInstruction = !isExternal && !isSource;
  const hasStoreInstruction = isStored && !isSource;
  const hasExternalDeriveInstruction = isExternal && !isSource;

  return [
    hasRefreshInstruction ? makeRefreshInstruction(model) : [],
    hasFetchInstruction ? makeFetchInstruction(model) : [],
    hasDeriveInstruction ? makeDeriveInstruction(model, indices) : [],
    hasStoreInstruction ? makeStoreInstruction(model) : [],
    hasExternalDeriveInstruction ? makeExternalDeriveInstruction(model, indices) : [],
  ].flat();
}

export function isSourceModel(model: ReltModelDefinition): boolean {
  const externalMod = model.modifiers.find(x => x.kind === "ReltExternalModelModifier") as ReltExternalModelModifier;
  const isOfTypeObj = model.expression.kind === "ReltTypeObjectExpression";
  if (externalMod === undefined)
    return isOfTypeObj;
  return externalMod.using.length === 0;
}

export function isTypeModel(model: ReltModelDefinition): boolean {
  return model.modifiers.some(x => x.kind === "ReltTypeModelModifier");
}

export function isExternalModel(model: ReltModelDefinition): boolean {
  return model.modifiers.some(x => x.kind === "ReltExternalModelModifier");
}

export function isStoredModel(model: ReltModelDefinition): boolean {
  return model.modifiers.some(x => match(x.kind)
    .with("ReltDeltaModelModifier", () => true)
    .with("ReltIndexModelModifier", () => false)
    .with("ReltPostgresModelModifier", () => true)
    .with("ReltTypeModelModifier", () => false)
    .with("ReltExternalModelModifier", () => false)
    .exhaustive()
  );
}

export function makeRefreshInstruction(model: ReltModelDefinition): RefreshInstruction {
  return {
    kind: "RefreshInstruction",
    name: "refresh",
    className: `Refresh${model.name}Instruction`,
    storageClassName: `${model.name}Storage`,
    deltaClassName: `${model.name}`,
  };
}

export function makeFetchInstruction(model: ReltModelDefinition): FetchInstruction {
  return {
    kind: "FetchInstruction",
    name: "fetch",
    className: `Fetch${model.name}Instruction`,
    storageClassName: `${model.name}Storage`,
    deltaClassName: `${model.name}`,
  };
}

export function makeDeriveInstruction(model: ReltModelDefinition, indices: Record<string, number>): DeriveInstruction {
  return {
    kind: "DeriveInstruction",
    name: "derive",
    className: `Derive${model.name}Instruction`,
    steps: makeDeriveSteps(model, indices),
  };
}

export function makeDeriveSteps(model: ReltModelDefinition, indices: Record<string, number>): DeriveStep[] {
  let c = 0;
  const stack: number[] = [];
  const steps: DeriveStep[] = [];

  const next = (): number => {
    const x = c++;
    stack.push(x);
    return x;
  };

  const helper = (x: ReltExpression) => {
    switch (x.kind) {
      case "ReltPipeExpression": {
        helper(x.left);
        helper(x.right);
        break;
      }
      case "ReltWhereExpression": {
        const ds = stack.pop();
        assertDefined(ds, ``);
        steps.push({
          kind: "WhereStep",
          condition: makeColumnExpression(x.condition),
          dest: next(),
          ds,
        });
        break;
      }
      case "ReltSortExpression": {
        const ds = stack.pop();
        assertDefined(ds, ``);
        steps.push({
          kind: "SortStep",
          dest: next(),
          ds,
          mode: x.op,
          columns: x.columns.map(makeColumnExpression),
        });
        break;
      }
      case "ReltOverExpression": {
        throw new Error('TODO');
      }
      case "ReltJoinExpression": {
        helper(x.other);
        const right = stack.pop();
        const left = stack.pop();
        assertDefined(left, ``);
        assertDefined(right, ``);
        steps.push({
          kind: "JoinStep",
          condition: x.on === undefined ? throws("Missing join op") : makeColumnExpression(x.on),
          dest: next(),
          left,
          right,
          type: x.op,
        });
        break;
      }
      case "ReltUnionExpression": {
        helper(x.other);
        const right = stack.pop();
        const left = stack.pop();
        assertDefined(left, ``);
        assertDefined(right, ``);
        steps.push({
          kind: "UnionStep",
          dest: next(),
          left,
          right,
        });
        break;
      }
      case "ReltWithExpression": {
        x.properties.forEach(x => {
          const ds = stack.pop();
          assertDefined(ds, ``);
          switch (x.kind) {
            case "ReltRenameObjectProperty": {
              steps.push({
                kind: "RenameStep",
                dest: next(),
                ds,
                name: x.name,
                value: makeColumnExpression(x.value),
              })
              break;
            }
            case "ReltAssignObjectProperty": {
              steps.push({
                kind: "WithStep",
                dest: next(),
                ds,
                name: x.name,
                value: makeColumnExpression(x.value),
              })
              break;
            }
            case "ReltOpAssignObjectProperty": {
              steps.push({
                kind: "WithStep",
                dest: next(),
                ds,
                name: x.name,
                value: makeColumnOpExpression(x.name, x.op, x.value),
              })
              break;
            }
            case "ReltAsObjectProperty": {
              steps.push({
                kind: "WithStep",
                dest: next(),
                ds,
                name: x.name,
                value: makeColumnTypeExpression(x.type),
              })
              break;
            }
          }
        })
        break;
      }
      case "ReltGroupExpression": {
        helper(x.value);
        break;
      }
      case "ReltIdentifierExpression": {
        steps.push({
          kind: "UseStep",
          dest: next(),
          dssIdx: indices[x.name],
        });
        break;
      }
      case "ReltOrExpression":
      case "ReltAndExpression":
      case "ReltCmpExpression":
      case "ReltAddExpression":
      case "ReltMulExpression":
      case "ReltDotExpression":
      case "ReltStringExpression":
      case "ReltIntegerExpression":
      case "ReltFloatExpression":
      case "ReltBooleanExpression":
      case "ReltEnvVarExpression":
      case "ReltTypeObjectExpression":
      case "ReltCoalesceExpression": {
        throws(`Expression ${x.kind} could not be converted to a derive step`);
      }
    }
  };

  helper(model.expression);
  const ds = stack.pop();
  assertDefined(ds, ``);

  steps.push({
    kind: "AsStep",
    dest: next(),
    ds,
    type: {
      kind: "ReltIdentifierType",
      name: model.name,
    },
  }, {
    kind: "RetStep",
    ds: stack.pop()!,
  });

  return steps;
}

export function makeColumnExpression(x: ReltExpression): ColumnExpression {
  switch (x.kind) {
    case "ReltPipeExpression":
    case "ReltWhereExpression":
    case "ReltSortExpression":
    case "ReltOverExpression":
    case "ReltJoinExpression":
    case "ReltUnionExpression":
    case "ReltWithExpression":
    case "ReltTypeObjectExpression": {
      throws(`Cannot convert ${x.kind} to a column expression`);
    }
    case "ReltOrExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnBinaryOpExpression",
        op: "||",
        left,
        right,
      };
    }
    case "ReltAndExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnBinaryOpExpression",
        op: "&&",
        left,
        right,
      };
    }
    case "ReltCmpExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnBinaryOpExpression",
        op: {
          "==": "===",
          "!=": "=!=",
          "<=": "<=",
          ">=": ">=",
          "<": "<",
          ">": ">"
        }[x.op],
        left,
        right,
      };
    }
    case "ReltAddExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnBinaryOpExpression",
        op: x.op, // TODO need to convert string + string to concat(string, string)
        left,
        right,
      };
    }
    case "ReltMulExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnBinaryOpExpression",
        op: x.op,
        left,
        right,
      };
    }
    case "ReltDotExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnBinaryOpExpression",
        op: x.op,
        left,
        right,
      };
    }
    case "ReltIntegerExpression":
    case "ReltFloatExpression":
    case "ReltBooleanExpression":
    case "ReltStringExpression": {
      return {
        kind: "ColumnLiteralExpression",
        value: x,
      };
    }
    case "ReltEnvVarExpression": {
      throws("TODO");
    }
    case "ReltIdentifierExpression": {
      return {
        kind: "ColumnColExpression",
        name: x.name,
      };
    }
    case "ReltGroupExpression": {
      return {
        kind: "ColumnGroupExpression",
        value: makeColumnExpression(x.value),
      };
    }
    case "ReltCoalesceExpression": {
      const left = makeColumnExpression(x.left);
      const right = makeColumnExpression(x.right);
      return {
        kind: "ColumnWhenExpression",
        condition: {
          kind: "ColumnAppExpression",
          func: "isNotNull",
          col: left,
          args: []
        },
        ifTrue: left,
        ifFalse: right,
      };
    }
  }
}

export function makeColumnTypeExpression(x: ReltType): ColumnExpression {
  switch (x.kind) {
    case "ReltArrayType":
    case "ReltJsonType":
    case "ReltOptionalType":
    case "ReltIdentifierType":
    case "ReltStructType":
      throws(`Cannot convert non primitive type to column cast type`);
    case "ReltBooleanType":
      return {
        kind: "ColumnIdentifierExpression",
        name: "BooleanType",
      };
    case "ReltDateType":
      return {
        kind: "ColumnIdentifierExpression",
        name: "DateType",
      };
    case "ReltFloatType":
      return {
        kind: "ColumnIdentifierExpression",
        name: "DoubleType",
      };
    case "ReltIntegerType":
      return {
        kind: "ColumnIdentifierExpression",
        name: "IntegerType",
      };
    case "ReltStringType":
      return {
        kind: "ColumnIdentifierExpression",
        name: "StringType",
      };
  }
}

export function makeColumnOpExpression(name: string, op: string, x: ReltExpression): ColumnExpression {
  switch (op) {
    case "??=":
      return makeColumnExpression({
        kind: "ReltCoalesceExpression",
        left: {
          kind: "ReltIdentifierExpression",
          name,
        },
        op: "??",
        right: x,
      })
    default:
      throws(`Unknown op ${op}`);
  }
}

export function makeStoreInstruction(model: ReltModelDefinition): StoreInstruction {
  return {
    kind: "StoreInstruction",
    name: "store",
    className: `Store${model.name}Instruction`,
    storageClassName: `${model.name}Storage`,
  };
}

export function getValue(x: ReltStringExpression | ReltEnvVarExpression): string {
  return match(x)
    .with({ kind: "ReltStringExpression" }, x => x.value)
    .with({ kind: "ReltEnvVarExpression" }, x => {
      const value = process.env[x.value];
      if (value === undefined)
        throws(`Env ${x.value} is not set`)
      return value;
    })
    .exhaustive();
}

function last<T>(x: T[]): T {
  return x.length > 0 ? x[x.length - 1] : throws(`Cannot get the last element of an empty list`);
}

export function makeExternalDeriveInstruction(model: ReltModelDefinition, indices: Record<string, number>): DeriveInstruction {
  const mod = model.modifiers.find(x => x.kind === "ReltExternalModelModifier")! as ReltExternalModelModifier;
  let c = 0;

  const externalClassValue = getValue(mod.value).trim();
  const externalClassName = last(externalClassValue.split('.'));

  return {
    kind: "DeriveInstruction",
    name: "derive",
    className: `Derive${model.name}Instruction`,
    steps: [
      ...mod.using.map<DeriveStep>(x => ({
        kind: "UseStep",
        dest: c++,
        dssIdx: indices[x.name],
      })),
      {
        kind: "CallStep",
        dest: c++,
        name: `${externalClassName}.execute`,
        args: [
          "spark",
          ...mod.using.map((_, i) => i)
        ],
      },
      {
        kind: "AsStep",
        ds: c - 1,
        dest: c++,
        type: {
          kind: "ReltIdentifierType",
          name: model.name,
        },
      }, {
        kind: "RetStep",
        ds: c - 1,
      }
    ],
  };
}
