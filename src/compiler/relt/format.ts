import { match } from 'ts-pattern';
import { ReltDefinition, ReltExpression, ReltModelModifier, ReltModule, ReltObjectProperty, ReltType } from "./types";

export function formatRelt(x: ReltModule | ReltDefinition | ReltExpression | ReltType | ReltModelModifier): string {

  const helper = (x: ReltModule | ReltDefinition | ReltExpression | ReltType | ReltModelModifier | ReltObjectProperty): string => {
    return match(x)
      .with({ kind: "ReltStringType" }, x => `string`)
      .with({ kind: "ReltBooleanType" }, x => `bool`)
      .with({ kind: "ReltIntegerType" }, x => `int`)
      .with({ kind: "ReltFloatType" }, x => `float`)
      .with({ kind: "ReltStructType" }, x => `struct`)
      .with({ kind: "ReltIdentifierType" }, x => x.name)
      .with({ kind: "ReltOptionalType" }, x => `${helper(x.of)}?`)
      .with({ kind: "ReltArrayType" }, x => `${helper(x.of)}[]`)
      .with({ kind: "ReltJsonType" }, x => `${helper(x.of)} json`)
      .with({ kind: "ReltDateType" }, x => `date${x.fmt === undefined ? "" : ` "${x.fmt}"`}`)
      .with({ kind: "ReltModule" }, x => x.definitions.map(helper).join('\n'))
      .with({ kind: "ReltModelDefinition" }, x => ``)
      .with({ kind: "ReltDeltaModelModifier" }, x => `delta ${helper(x.value)}`)
      .with({ kind: "ReltPostgresModelModifier" }, x => `posgtres ${helper(x.value)}`)
      .with({ kind: "ReltIndexModelModifier" }, x => `index ${helper(x.value)} on ${helper(x.on)}`)
      .with({ kind: "ReltTypeModelModifier" }, x => `type`)
      .with({ kind: "ReltPipeExpression" }, x => `${helper(x.left)}\n|${helper(x.right)}`)
      .with({ kind: "ReltWhereExpression" }, x => `where ${helper(x.condition)}`)
      .with({ kind: "ReltSortExpression" }, x => `sort ${x.columns.map(helper).join(', ')} ${x.op}`)
      .with({ kind: "ReltOverExpression" }, x => `over ${helper(x.column)}`)
      .with({ kind: "ReltJoinExpression" }, x => `${x.op} join${x.on === undefined ? "" : ` ${helper(x.on)}`} ${helper(x.other)}`)
      .with({ kind: "ReltUnionExpression" }, x => `union ${helper(x.other)}`)
      .with({ kind: "ReltWithExpression" }, x => `with {${x.properties.map(x => `${helper(x)},\n`)}}`)
      .with({ kind: "ReltOrExpression" }, x => `${helper(x.left)} ${x.op} ${helper(x.right)}`)
      .with({ kind: "ReltAndExpression" }, x => `${helper(x.left)} ${x.op} ${helper(x.right)}`)
      .with({ kind: "ReltCmpExpression" }, x => `${helper(x.left)} ${x.op} ${helper(x.right)}`)
      .with({ kind: "ReltAddExpression" }, x => `${helper(x.left)} ${x.op} ${helper(x.right)}`)
      .with({ kind: "ReltMulExpression" }, x => `${helper(x.left)} ${x.op} ${helper(x.right)}`)
      .with({ kind: "ReltDotExpression" }, x => `${helper(x.left)}${x.op}${helper(x.right)}`)
      .with({ kind: "ReltCoalesceExpression" }, x => `${helper(x.left)} ${x.op} ${helper(x.right)}`)
      .with({ kind: "ReltStringExpression" }, x => `"${x.value}"`)
      .with({ kind: "ReltIntegerExpression" }, x => `${x.value}`)
      .with({ kind: "ReltFloatExpression" }, x => `${x.value}`)
      .with({ kind: "ReltBooleanExpression" }, x => `${x.value}`)
      .with({ kind: "ReltEnvVarExpression" }, x => `$"${x.value}"`)
      .with({ kind: "ReltIdentifierExpression" }, x => `${x.name}`)
      .with({ kind: "ReltTypeObjectExpression" }, x => `{${x.properties.map(x => `${x.name}: ${helper(x.type)},\n`)}}`)
      .with({ kind: "ReltGroupExpression" }, x => `(${helper(x.value)})`)
      .with({ kind: "ReltAssignObjectProperty" }, x => ``)
      .with({ kind: "ReltAsObjectProperty" }, x => ``)
      .with({ kind: "ReltOpAssignObjectProperty" }, x => ``)
      .with({ kind: "ReltRenameObjectProperty" }, x => ``)
      .exhaustive();
  };

  return helper(x);
}
