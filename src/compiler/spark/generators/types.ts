import { match } from "ts-pattern";
import { throws } from "../../../errors";
import { tc } from "../../relt/typechecker";
import { ReltModelDefinition, ReltType } from "../../relt/types";
import { ScalaCaseClassDefinition, ScalaObjectDefinition, ScalaType } from "../types";

export function makeTypes(models: ReltModelDefinition[]): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: "Types",
    properties: models.map(makeType),
  };
}

export function makeType(model: ReltModelDefinition): ScalaCaseClassDefinition {
  const type = tc.typeCheck(model);
  return {
    kind: "ScalaCaseClassDefinition",
    name: model.name,
    properties: type.properties.map(x => ({
      name: x.name,
      type: convertType(x.type)
    })),
  };
}

export function convertType(type: ReltType): ScalaType {
  return match(type)
    .with({ kind: "ReltArrayType" }, x => ({ kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Array" }, of: [convertType(x.of)] }) satisfies ScalaType)
    .with({ kind: "ReltBooleanType" }, x => ({ kind: "ScalaBooleanType" }) satisfies ScalaType)
    .with({ kind: "ReltDateType" }, x => ({ kind: "ScalaDateType" }) satisfies ScalaType)
    .with({ kind: "ReltFloatType" }, x => ({ kind: "ScalaDoubleType" }) satisfies ScalaType)
    .with({ kind: "ReltIdentifierType" }, x => ({ kind: "ScalaIdentifierType", name: x.name }) satisfies ScalaType)
    .with({ kind: "ReltIntegerType" }, x => ({ kind: "ScalaIntType" }) satisfies ScalaType)
    .with({ kind: "ReltJsonType" }, x => convertType(x.of) satisfies ScalaType)
    .with({ kind: "ReltOptionalType" }, x => ({ kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Option" }, of: [convertType(x.of)] }) satisfies ScalaType)
    .with({ kind: "ReltStringType" }, x => ({ kind: "ScalaStringType" }) satisfies ScalaType)
    .with({ kind: "ReltStructType" }, x => throws(`TODO: convert struct type to scala type, this should implicitly generate a scala type`))
    .exhaustive();
}
