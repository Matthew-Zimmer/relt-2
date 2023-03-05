import { match } from "ts-pattern";
import { ScalaDefinition, ScalaExpression, ScalaType } from "./types";

export function formatScala(x: ScalaExpression | ScalaDefinition | ScalaType): string {
  let indentation = "";

  const block = (x: (ScalaExpression | ScalaDefinition | ScalaType)[], suffix: string = ""): string => {
    indentation += "  ";
    const r = x.map(x => `${indentation}${helper(x)}${suffix}\n`).join('');
    indentation = indentation.slice(0, -2);
    return r;
  };

  const helper = (x: ScalaExpression | ScalaDefinition | ScalaType): string => {
    return match(x)
      .with({ kind: "ScalaObjectDefinition" }, x => `object ${x.name}${x.extends ? ` extends ${x.extends}` : ""} {${block(x.properties)}\n${indentation}}`)
      .with({ kind: "ScalaClassDefinition" }, x => ``)
      .with({ kind: "ScalaTraitDefinition" }, x => ``)
      .with({ kind: "ScalaTypeDefinition" }, x => ``)
      .with({ kind: "ScalaVarExpression" }, x => `var ${x.name} = ${helper(x.value)}`)
      .with({ kind: "ScalaValExpression" }, x => `val ${x.name} = ${helper(x.value)}`)
      .with({ kind: "ScalaBinaryOpExpression" }, x => `(${helper(x.left)} ${x.op} ${helper(x.right)})`)
      .with({ kind: "ScalaAppExpression" }, x => `${helper(x.func)}${x.args.map(helper).join(', ')}`)
      .with({ kind: "ScalaDotExpression" }, x => `${helper(x.left)}.${helper(x.right)}`)
      .with({ kind: "ScalaIdentifierExpression" }, x => `${x.name}${x.types === undefined ? "" : `[${x.types.map(helper).join(', ')}]`}`)
      .with({ kind: "ScalaDefExpression" }, x => `def ${x.name}(${x.args.map(x => `${x.name}: ${helper(x.type)}`).join(', ')}): ${helper(x.returnType)} = ${helper(x.body)}`)
      .with({ kind: "ScalaGroupExpression" }, x => `{${block(x.expressions)}\n${indentation}}`)
      .with({ kind: "ScalaNewExpression" }, x => `new ${helper(x.value)}`)
      .with({ kind: "ScalaDoubleType" }, x => `Double`)
      .with({ kind: "ScalaIntType" }, x => `Int`)
      .with({ kind: "ScalaBooleanType" }, x => `Boolean`)
      .with({ kind: "ScalaStringType" }, x => `String`)
      .with({ kind: "ScalaIdentifierType" }, x => x.name)
      .with({ kind: "ScalaDotType" }, x => `${helper(x.left)}.${helper(x.right)}`)
      .with({ kind: "ScalaOfType" }, x => `${helper(x.type)}[${x.of.map(helper).join(', ')}]`)
      .with({ kind: "ScalaDateType" }, x => `Date`)
      .exhaustive();
  };

  return helper(x);
}
