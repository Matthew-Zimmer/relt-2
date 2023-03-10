import { match, P } from "ts-pattern";
import { throws } from "../../errors";
import { ScalaDefinition, ScalaExpression, ScalaType } from "./types";

export function formatScala(x: ScalaExpression | ScalaDefinition | ScalaType): string {
  let indentation = "";

  const block = (x: (ScalaExpression | ScalaDefinition | ScalaType)[], suffix: string = "", join: string = ""): string => basicBlock(x, helper, suffix, join);

  const basicBlock = <T>(x: T[], f: (x: T) => string, suffix: string = "", join: string = ""): string => {
    indentation += "  ";
    const r = x.map(x => `${indentation}${f(x)}${suffix}\n`).join(join);
    indentation = indentation.slice(0, -2);
    return r;
  };

  const helper = (x: ScalaExpression | ScalaDefinition | ScalaType): string => {
    return match(x)
      .with({ kind: "ScalaObjectDefinition" }, x => `object ${x.name}${x.extends ? ` extends ${x.extends}` : ""} {\n${block(x.properties, "", "\n")}${indentation}}`)
      .with({ kind: "ScalaClassDefinition" }, x => ``)
      .with({ kind: "ScalaTraitDefinition" }, x => ``)
      .with({ kind: "ScalaTypeDefinition" }, x => `type ${x.name} = ${helper(x.type)}`)
      .with({ kind: "ScalaCaseClassDefinition" }, x => `case class ${x.name}(\n${basicBlock(x.properties, x => `${x.name}: ${helper(x.type)}`, ",")}${indentation})`)
      .with({ kind: "ScalaVarExpression" }, x => `${x.visibility === undefined ? "" : x.visibility + " "}var ${x.name} = ${helper(x.value)}`)
      .with({ kind: "ScalaValExpression" }, x => `${x.visibility === undefined ? "" : x.visibility + " "}val ${x.name} = ${helper(x.value)}`)
      .with({ kind: "ScalaBinaryOpExpression" }, x => `(${helper(x.left)} ${x.op} ${helper(x.right)})`)
      .with({ kind: "ScalaAppExpression" }, x => `${helper(x.func)}(${x.hints?.indent ? "\n" + block(x.args, ",") + indentation : x.args.map(helper).join(', ')})`)
      .with({ kind: "ScalaDotExpression" }, x => `${helper(x.left)}${x.hints?.indent ? `\n${indentation}  ` : ""}.${helper(x.right)}`)
      .with({ kind: "ScalaIdentifierExpression" }, x => `${x.name}${x.types === undefined ? "" : `[${x.types.map(helper).join(', ')}]`}`)
      .with({ kind: "ScalaDefExpression" }, x => `def ${x.name}(${x.args.map(x => `${x.name}: ${helper(x.type)}`).join(', ')}): ${helper(x.returnType)} = ${helper(x.body)}`)
      .with({ kind: "ScalaGroupExpression" }, x => `{\n${block(x.expressions, "", "\n")}${indentation}}`)
      .with({ kind: "ScalaNewExpression" }, x => `new ${helper(x.value)}`)
      .with({ kind: "ScalaDoubleType" }, x => `Double`)
      .with({ kind: "ScalaIntType" }, x => `Int`)
      .with({ kind: "ScalaBooleanType" }, x => `Boolean`)
      .with({ kind: "ScalaStringType" }, x => `String`)
      .with({ kind: "ScalaIdentifierType" }, x => x.name)
      .with({ kind: "ScalaDotType" }, x => `${helper(x.left)}.${helper(x.right)}`)
      .with({ kind: "ScalaOfType" }, x => `${helper(x.type)}[${x.of.map(helper).join(', ')}]`)
      .with({ kind: "ScalaDateType" }, x => `Date`)
      .with({ kind: "ScalaTupleType" }, x => match(x.types)
        .with([], () => throws(`In scala empty tuple types are invalid`))
        .with([P.select()], x => `Tuple1(${helper(x)})`)
        .otherwise(x => `(${x.map(helper).join(', ')})`)
      )
      .with({ kind: "ScalaStringExpression" }, x => `"${x.value}"`)
      .with({ kind: "ScalaBooleanExpression" }, x => `${x.value}`)
      .with({ kind: "ScalaFloatExpression" }, x => `${x.value}`)
      .with({ kind: "ScalaIntegerExpression" }, x => `${x.value}`)
      .with({ kind: "ScalaImportExpression" }, x => `import ${helper(x.value)}`)
      .with({ kind: "ScalaReturnExpression" }, x => `return ${helper(x.value)}`)
      .with({ kind: "ScalaTupleExpression" }, x => match(x.values)
        .with([], () => throws(`In scala empty tuples are invalid`))
        .with([P.select()], x => `tuple1(${helper(x)})`)
        .otherwise(x => `(${x.map(helper).join(', ')})`)
      )
      .with({ kind: "ScalaPartExpression" }, x => `(${helper(x.value)})`)
      .exhaustive();
  };

  return helper(x);
}
