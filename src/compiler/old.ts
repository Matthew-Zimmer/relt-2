// CHILDREN

// export function children(e: Expression): Expression[] {
//   switch (e.kind) {
//     case "StringExpression":
//     case "IntegerExpression":
//     case "FloatExpression":
//     case "BooleanExpression":
//     case "EnvVarExpression":
//     case "IdentifierExpression":
//     case "TypeObjectExpression":
//       return [];
//     case "GroupExpression":
//       return [e.value];
//     case "PipeExpression":
//     case "OrExpression":
//     case "AndExpression":
//     case "CmpExpression":
//     case "AddExpression":
//     case "MulExpression":
//     case "DotExpression":
//     case "CoalesceExpression":
//       return [e.left, e.right]
//     case "WhereExpression": return e.head === undefined ? [e.condition] : [e.head, e.condition];
//     case "SortExpression": return e.head === undefined ? e.columns : [e.head, ...e.columns];
//     case "OverExpression": return e.head === undefined ? [e.column] : [e.head, e.column];
//     case "UnionExpression": return e.head === undefined ? [e.other] : [e.head, e.other];
//     case "JoinExpression": {
//       if (e.head === undefined && e.on === undefined) return [e.other];
//       else if (e.head === undefined) return [e.on!, e.other];
//       else if (e.on === undefined) return [e.head, e.other];
//       else return [e.head, e.on, e.other];
//     }
//     case "WithExpression": {
//       const fromProps = (x: ObjectProperty): Expression[] => {
//         switch (x.kind) {
//           case "AsObjectProperty": return [];
//           case "AssignObjectProperty":
//           case "OpAssignObjectProperty":
//           case "RenameObjectProperty":
//             return [x.value];
//         }
//       }
//       return e.head === undefined ? e.properties.flatMap(fromProps) : [e.head, ...e.properties.flatMap(fromProps)];
//     }
//   }
// }

// export function fromChildren(e: Expression, children: Expression[]): Expression {
//   switch (e.kind) {
//     case "StringExpression":
//     case "IntegerExpression":
//     case "FloatExpression":
//     case "BooleanExpression":
//     case "EnvVarExpression":
//     case "IdentifierExpression":
//     case "TypeObjectExpression":
//       return e;
//     case "GroupExpression":
//       return { ...e, value: children[0] };
//     case "PipeExpression":
//     case "OrExpression":
//     case "AndExpression":
//     case "CmpExpression":
//     case "AddExpression":
//     case "MulExpression":
//     case "DotExpression":
//     case "CoalesceExpression":
//       return { ...e, left: children[0], right: children[1] };
//     case "WhereExpression": return e.head === undefined ? { ...e, condition: children[0] } : { ...e, head: children[0], condition: children[1] };
//     case "SortExpression": return e.head === undefined ? { ...e, columns: children as IdentifierExpression[] } : { ...e, head: children[0], columns: children.slice(1) as IdentifierExpression[] };
//     case "OverExpression": return e.head === undefined ? { ...e, column: children[0] } : { ...e, head: children[0], column: children[1] };
//     case "UnionExpression": return e.head === undefined ? { ...e, other: children[0] } : { ...e, head: children[0], other: children[1] };
//     case "JoinExpression": {
//       if (e.head === undefined && e.on === undefined) return { ...e, other: children[0] };
//       else if (e.head === undefined) return { ...e, on: children[0] as GroupExpression, other: children[1] };
//       else if (e.on === undefined) return { ...e, head: children[0], other: children[1] };
//       else return { ...e, head: children[0], on: children[1] as GroupExpression, other: children[2] };
//     }
//     case "WithExpression": {
//       const makeProps = (x: ObjectProperty, i: number): ObjectProperty => {
//         switch (x.kind) {
//           case "AsObjectProperty":
//             return x;
//           case "AssignObjectProperty":
//           case "OpAssignObjectProperty":
//           case "RenameObjectProperty":
//             return { ...x, value: children[i] };
//         }
//       };
//       return e.head === undefined ?
//         { ...e, properties: e.properties.map(makeProps) } :
//         { ...e, head: children[0], properties: e.properties.map(makeProps) };
//     }
//   }
// }

// export function applyToChildren(e: Expression, f: (x: Expression) => Expression): Expression {
//   return fromChildren(e, children(e).map(f));
// }


// SCALA

// export interface ScalaCaseClass {
//   name: string;
//   properties: { name: string, type: ScalaType }[];
// }

// export type ScalaType =
//   | { kind: "ScalaDoubleType" }
//   | { kind: "ScalaIntType" }
//   | { kind: "ScalaBooleanType" }
//   | { kind: "ScalaStringType" }
//   | { kind: "ScalaIdentifierType", name: string }
//   | { kind: "ScalaDotType", left: ScalaType, right: ScalaType }
//   | { kind: "ScalaOfType", type: ScalaType, of: ScalaType[] }
//   | { kind: "ScalaDateType" }

// export interface SparkHandler {
//   name: string;
//   typeName: string;
//   combine: string;
//   create?: SparkTableFunction;
//   storage?: SparkStorage;
//   plans: SparkPlans;
//   consumes: string[];
//   feeds: string[];
//   hasCache: boolean;
// }

// export interface SparkStorage {
//   name: string;
//   args: string[];
// }

// export interface SparkPlan {
//   args: string[];
//   typeArgs: string[];
// }

// export interface SparkPlans {
//   invalidatePlan?: SparkPlan;
//   storePlan?: SparkPlan;
//   refreshPlan?: SparkPlan;
//   fetchPlan?: SparkPlan;
//   derivePlan?: SparkPlan;
// }

// export type SparkTableFunction =
//   | { kind: "SparkJoinFunction", lIdx: number, rIdx: number, on: SparkExpression, type: "inner" | "left" | "right", typeName: string }
//   | { kind: "SparkUnionFunction", lIdx: number, rIdx: number, }
//   | { kind: "SparkWhereFunction", idx: number, cond: SparkExpression }
//   | { kind: "SparkSortFunction", idx: number, columns: string[], type: "asc" | "desc" }
//   | { kind: "SparkWithColumnFunction", idx: number, properties: { name: string, value: SparkExpression, rename: boolean }[], typeName: string }

// export type SparkExpression =
//   | { kind: "SparkLiteralExpression", value: string }
//   | { kind: "SparkColumnExpression", value: string }
//   | { kind: "SparkGroupExpression", value: SparkExpression }
//   | { kind: "SparkBinaryExpression", left: SparkExpression, op: string, right: SparkExpression }
//   | { kind: "SparkAppExpression", func: string, args: SparkExpression[] }
//   | { kind: "SparkRawExpression", value: string }

// function toScalaCaseClass(x: ModelDefinition): ScalaCaseClass {
//   const t = tc.typeCheck(x);
//   return { name: x.name, properties: t.properties.map(x => ({ name: x.name, type: toScalaType(x.type) })) };
// }

// function toScalaType(x: Type): ScalaType {
//   switch (x.kind) {
//     case "BooleanType": return { kind: "ScalaBooleanType" };
//     case "FloatType": return { kind: "ScalaDoubleType" };
//     case "IntegerType": return { kind: "ScalaIntType" };
//     case "StringType": return { kind: "ScalaStringType" };
//     case "StructType": assertInvariant(false, 'Should not be able to convert a struct type to scala'); throw '';
//     case "IdentifierType": return { kind: "ScalaDotType", left: { kind: "ScalaIdentifierType", name: "Types" }, right: { kind: "ScalaIdentifierType", name: x.name } };
//     case "JsonType": return toScalaType(x.of);
//     case "DateType": return { kind: "ScalaDateType" };
//     case "ArrayType": return { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Array" }, of: toScalaType(x.of) };
//     case "OptionalType": return { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Option" }, of: toScalaType(x.of) };
//   }
// }

// function createStorage(x: ModelDefinition): SparkStorage | undefined {
//   let storage: SparkStorage | undefined = undefined;

//   for (const m of x.modifiers) {
//     switch (m.kind) {
//       case "DeltaModelModifier": {
//         assertExpectation(storage === undefined, `Cannot have multiple storage modifiers`);
//         const name = `DeltaFileStorage`;
//         switch (m.value.kind) {
//           case "StringExpression":
//             storage = {
//               name,
//               args: [`"${m.value.value}"`],
//             };
//             break;
//           case "EnvVarExpression":
//             storage = {
//               name,
//               args: [`"${m.value.value}"`], // TODO emit scala code which reads the env
//             };
//             break;
//         }
//         break;
//       }
//       case "PostgresModelModifier": {
//         assertExpectation(storage === undefined, `Cannot have multiple storage modifiers`);
//         const name = `PostgresTableStorage`;
//         switch (m.value.kind) {
//           case "StringExpression":
//             storage = {
//               name,
//               args: [`"${m.value.value}"`],
//             };
//             break;
//           case "EnvVarExpression":
//             storage = {
//               name,
//               args: [`"${m.value.value}"`], // TODO emit scala code which reads the env
//             };
//             break;
//         }
//         break;
//       }
//       case "IndexModelModifier": {
//         assertExpectation(storage === undefined, `Cannot have multiple storage modifiers`);
//         const name = `IndexerStorage`;
//         switch (m.value.kind) {
//           case "StringExpression":
//             storage = {
//               name,
//               args: [`"${m.value.value}"`, `"${m.on.value}"`],
//             };
//             break;
//           case "EnvVarExpression":
//             storage = {
//               name,
//               args: [`"${m.value.value}"`, `"${m.on.value}"`], // TODO emit scala code which reads the env
//             };
//             break;
//         }
//         break;
//       }
//       case "TypeModelModifier":
//         break;
//     }
//   }

//   return storage;
// }

// function isSourceType(x: ModelDefinition): boolean {
//   if (x.modifiers.some(x => x.kind === "TypeModelModifier"))
//     return false;

//   const expr = (x: Expression): boolean => {
//     switch (x.kind) {
//       case "TypeObjectExpression": return true;
//       default: return children(x).some(expr);
//     }
//   }
//   return expr(x.expression);
// }

// function makePlans(x: ModelDefinition, isSource: boolean, hasCache: boolean): SparkPlans {
//   const mods = new Set(x.modifiers.map(x => x.kind));

//   if (mods.has('TypeModelModifier')) return {};

//   const hasInvalidate = !isSource && hasCache && !mods.has('IndexModelModifier');
//   const hasStore = hasCache;
//   const hasFetch = isSource || hasCache;
//   const hasRefresh = isSource;
//   const hasDerive = !isSource;

//   return {
//     ...!hasInvalidate ? {} : {
//       invalidatePlan: { args: ["storage"], typeArgs: ['DeltaTypes', 'Types'] },
//     },
//     ...!hasStore ? {} : {
//       storePlan: { args: ["storage", "create", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
//     },
//     ...!hasFetch ? {} : {
//       fetchPlan: { args: ["storage", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
//     },
//     ...!hasRefresh ? {} : {
//       refreshPlan: { args: ["storage", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
//     },
//     ...!hasDerive ? {} : {
//       derivePlan: { args: ["create", "combine"], typeArgs: ['DeltaTypes'] }
//     },
//   };
// }

// function makeCreateFunction(x: ModelDefinition, indices: Map<string, number>, mapping: Map<string, string>): SparkTableFunction {
//   switch (x.expression.kind) {
//     case "JoinExpression":
//       return {
//         kind: "SparkJoinFunction",
//         lIdx: indices.get((x.expression.head as IdentifierExpression).name)!,
//         on: x.expression.on === undefined ? (() => { assertInvariant(false, `Cannot make join without on condition`); throw ''; })() : makeSparkExpression(x.expression.on),
//         rIdx: indices.get((x.expression.other as IdentifierExpression).name)!,
//         type: x.expression.op,
//         typeName: mapping.get(x.name)!,
//       };
//     case "SortExpression":
//       return {
//         kind: "SparkSortFunction",
//         idx: indices.get((x.expression.head as IdentifierExpression).name)!,
//         columns: x.expression.columns.map(x => x.name),
//         type: "asc"
//       };
//     case "UnionExpression":
//       return {
//         kind: "SparkUnionFunction",
//         lIdx: indices.get((x.expression.head as IdentifierExpression).name)!,
//         rIdx: indices.get((x.expression.other as IdentifierExpression).name)!,
//       };
//     case "WhereExpression":
//       return {
//         kind: "SparkWhereFunction",
//         idx: indices.get((x.expression.head as IdentifierExpression).name)!,
//         cond: makeSparkExpression(x.expression.condition),
//       };
//     case "WithExpression": {
//       const oldType = tc.typeCheck(x);
//       return {
//         kind: "SparkWithColumnFunction",
//         idx: indices.get((x.expression.head as IdentifierExpression).name)!,
//         properties: x.expression.properties.map(x => {
//           switch (x.kind) {
//             case "AsObjectProperty": {
//               const oldTy = oldType.properties.find(y => y.name === x.name)!.type;
//               return { name: x.name, rename: false, value: makeCastSparkExpression(x.name, oldTy, x.type) };
//             }
//             case "AssignObjectProperty":
//               return { name: x.name, rename: false, value: makeSparkExpression(x.value) };
//             case "OpAssignObjectProperty":
//               return { name: x.name, rename: false, value: makeOpSparkExpression(x.op, x.value) };
//             case "RenameObjectProperty":
//               return { name: x.name, rename: true, value: makeSparkExpression(x.value) };
//           }
//         }),
//         typeName: mapping.get(x.name)!,
//       };
//     }
//     case "OverExpression":
//       assertInvariant(false, `TODO`); throw '';
//     default:
//       assertInvariant(false, `Cannot create a creation function for model which does not have a table expression`); throw '';
//   }
// }

// function makeSparkExpression(x: Expression): SparkExpression {
//   switch (x.kind) {
//     case "BooleanExpression": return { kind: "SparkLiteralExpression", value: `${x.value}` };
//     case "EnvVarExpression": return { kind: "SparkLiteralExpression", value: `"${x.value}"` }; // TODO get env in scala
//     case "IntegerExpression": return { kind: "SparkLiteralExpression", value: `${x.value}` };
//     case "StringExpression": return { kind: "SparkLiteralExpression", value: `"${x.value}"` };
//     case "FloatExpression": return { kind: "SparkLiteralExpression", value: `${x.value}` };
//     case "GroupExpression": return { kind: "SparkGroupExpression", value: makeSparkExpression(x.value) };
//     case "IdentifierExpression": return { kind: "SparkColumnExpression", value: x.name };
//     case "AddExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: "and", right: makeSparkExpression(x.right) };
//     case "AndExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: "and", right: makeSparkExpression(x.right) };
//     case "CmpExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: { "==": "===", "!=": "!=", "<=": "<=", ">=": ">=", "<": "<", ">": ">" }[x.op], right: makeSparkExpression(x.right) };
//     case "MulExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: x.op, right: makeSparkExpression(x.right) };
//     case "OrExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: x.op, right: makeSparkExpression(x.right) };
//     case "CoalesceExpression": return { kind: "SparkBinaryExpression", left: makeSparkExpression(x.left), op: ".", right: { kind: "SparkAppExpression", func: "getOrElse", args: [makeSparkExpression(x.right)] } };

//     case "OverExpression":
//     case "PipeExpression":
//     case "SortExpression":
//     case "TypeObjectExpression":
//     case "UnionExpression":
//     case "WhereExpression":
//     case "WithExpression":
//     case "JoinExpression":
//     case "DotExpression":
//       assertInvariant(false, `Cannot convert ${x.kind} to spark expression`); throw '';
//   }
// }

// function makeOpSparkExpression(op: string, x: Expression): SparkExpression {
//   return match([op, x])
//     .with(["??=", { kind: "AndExpression" }], ([, r]) => {
//       return { kind: "SparkLiteralExpression", value: "TODO" } satisfies SparkExpression;
//     })
//     .with(P._, () => { throw new InternalError(``) })
//     .exhaustive();
// }

// function cast(name: string, typeName: string): SparkExpression {
//   return {
//     kind: "SparkBinaryExpression",
//     left: {
//       kind: "SparkColumnExpression",
//       value: name,
//     },
//     op: ".",
//     right: {
//       kind: "SparkAppExpression",
//       func: "cast",
//       args: [{
//         kind: "SparkRawExpression",
//         value: typeName
//       }]
//     },
//   };
// }

// function makeCastSparkExpression(name: string, oldType: Type, type: Type): SparkExpression {
//   return match([oldType, type])
//     .with([{ kind: "StringType" }, { kind: "StringType" }], ([l, r]) => {
//       console.warn(`Casting string to string`);
//       return { kind: "SparkColumnExpression", value: name } satisfies SparkExpression;
//     })
//     .with([{ kind: "StringType" }, { kind: "IntegerType" }], ([l, r]) => {
//       return cast(name, "IntType");
//     })
//     .with([{ kind: "StringType" }, { kind: "FloatType" }], ([l, r]) => {
//       return cast(name, "FloatType");
//     })
//     .with([{ kind: "StringType" }, { kind: "OptionalType", of: { kind: "StringType" } }], ([l, r]) => {
//       return { kind: "SparkColumnExpression", value: name } satisfies SparkExpression;
//     })
//     .with([{ kind: "IntegerType" }, { kind: "StringType" }], ([l, r]) => {
//       return cast(name, "StringType");
//     })
//     .with([{ kind: "IntegerType" }, { kind: "FloatType" }], ([l, r]) => {
//       return cast(name, "FloatType");
//     })
//     .with([{ kind: "IntegerType" }, { kind: "IntegerType" }], ([l, r]) => {
//       console.warn(`Casting integer to integer`);
//       return { kind: "SparkColumnExpression", value: name } satisfies SparkExpression;
//     })
//     .with([{ kind: "IntegerType" }, { kind: "OptionalType", of: { kind: "IntegerType" } }], ([l, r]) => {
//       return { kind: "SparkColumnExpression", value: name } satisfies SparkExpression;
//     })
//     .with([{ kind: "FloatType" }, { kind: "StringType" }], ([l, r]) => {
//       return cast(name, "StringType");
//     })
//     .with([{ kind: "FloatType" }, { kind: "FloatType" }], ([l, r]) => {
//       console.warn(`Casting float to float`);
//       return { kind: "SparkColumnExpression", value: name } satisfies SparkExpression;
//     })
//     .with([{ kind: "FloatType" }, { kind: "IntegerType" }], ([l, r]) => {
//       return cast(name, "IntType");
//     })
//     .with([{ kind: "FloatType" }, { kind: "OptionalType", of: { kind: "FloatType" } }], ([l, r]) => {
//       return { kind: "SparkColumnExpression", value: name } satisfies SparkExpression;
//     })
//     .with(P._, ([l, r]) => { throw new InternalError(`Cannot cast ${formatType(l)} to ${formatType(r)}`) })
//     .exhaustive();
// }

// function toSparkHandler(x: ModelDefinition, indices: Map<string, number>, mapping: Map<string, string>): SparkHandler {
//   const idx = indices.get(x.name)!;
//   const count = mapping.size;
//   const storage = createStorage(x);
//   const isSource = isSourceType(x);
//   const hasCache = storage !== undefined && !isSource;
//   const typeName = mapping.get(x.name)!;
//   return {
//     name: x.name,
//     typeName,
//     combine: `(${Array.from({ length: count }).map((_, i) => i === idx ? "ds" : `dss._${i + 1}`).join(', ')})`,
//     feeds: deps.feedsInto(x.name),
//     consumes: deps.consumeFrom(x.name),
//     hasCache,
//     plans: makePlans(x, isSource, hasCache),
//     storage,
//     create: isSource ? undefined : makeCreateFunction(x, indices, mapping),
//   };
// }

// function addDeltaProperty(x: ScalaCaseClass): ScalaCaseClass {
//   return {
//     ...x,
//     properties: [
//       ...x.properties,
//       {
//         name: "__delta_state_kind",
//         type: {
//           kind: "ScalaDotType",
//           left: { kind: "ScalaIdentifierType", name: "DeltaState" },
//           right: { kind: "ScalaIdentifierType", name: "Kind" }
//         }
//       },
//     ]
//   };
// }

// function formatScalaType(x: ScalaType): string {
//   switch (x.kind) {
//     case "ScalaBooleanType": return "Boolean";
//     case "ScalaDotType": return `${formatScalaType(x.left)}.${formatScalaType(x.right)}`;
//     case "ScalaDoubleType": return "Double";
//     case "ScalaIdentifierType": return x.name;
//     case "ScalaIntType": return "Int";
//     case "ScalaStringType": return "String";
//     case "ScalaOfType": return `${formatScalaType(x.type)}[${formatScalaType(x.of)}]`;
//     case "ScalaDateType": return `Date`;
//   }
// }

// function formatScalaCaseClass(x: ScalaCaseClass): string {
//   return `case class ${x.name} (\n${x.properties.map(end(x => `  ${x.name}: ${formatScalaType(x.type)}`, ',\n')).join('')})`;
// }

// function formatSparkHandler(x: SparkHandler): string {
//   return `object ${x.name} extends Handler[DeltaTypes.Datasets] {
//     def combine(dss: DeltaTypes.Datasets, ds: Dataset[DeltaTypes.${x.typeName}]): DeltaTypes.Datasets = {
//       return ${x.combine}
//     }

//     ${x.create === undefined ? "" :
//       `private def create(spark: SparkSession, dss: DeltaTypes.Datasets): Dataset[DeltaTypes.${x.typeName}] = {
//         import spark.implicits._
//       return ${formatSparkTableFunction(x.create)}
//     }`}

//     ${x.storage === undefined ? "" :
//       `private val storage = new ${x.storage.name}[Types.${x.typeName}](${x.storage.args.join(', ')})`
//     }

//     ${Object.entries(x.plans).map(([k, v]: [string, SparkPlan]) => `private val ${k} = new ${cap(k)}[DeltaTypes.Datasets, ${v.typeArgs.map(a => `${a}.${x.typeName}`)}](${v.args.map(x => `this.${x}`).join(',')})\n`).join('')}

//     val name = "${x.name}"
//     val consumes = Array[String](${x.consumes.map(quote).join(', ')})
//     val feeds = Array[String](${x.consumes.map(quote).join(', ')})
//     val hasCache = ${x.hasCache}
//     val plans = Map(
//       ${Object.keys(x.plans).map(k => `${quote(cap(k))} -> this.${k},\n`).join('')}
//     )
//   }`;
// }

// function formatSparkTableFunction(x: SparkTableFunction): string {
//   switch (x.kind) {
//     case "SparkJoinFunction": return `dss._${x.lIdx + 1}.join(dss._${x.rIdx + 1}, ${formatSparkExpression(x.on)}, "${x.type}").as[DeltaTypes.${x.typeName}]`; // try without as
//     case "SparkSortFunction": return `dss._${x.idx + 1}.orderBy(${x.columns.map(c => `col("${c}").${x.type}`)})`; // try without as
//     case "SparkUnionFunction": return `dss._${x.lIdx + 1}.union(dss._${x.rIdx + 1})`; // try without as
//     case "SparkWhereFunction": return `dss._${x.idx + 1}.where(${formatSparkExpression(x.cond)})`; // try without as  
//     case "SparkWithColumnFunction": return `dss._${x.idx + 1}${x.properties.map(x => `.withColumn("${x.name}", ${formatSparkExpression(x.value)})`)}.as[DeltaTypes.${x.typeName}]`;
//   }
// }

// function formatSparkExpression(x: SparkExpression): string {
//   switch (x.kind) {
//     case "SparkBinaryExpression": return `(${formatSparkExpression(x.left)} ${x.op} ${formatSparkExpression(x.right)})`
//     case "SparkColumnExpression": return `col("${x.value}")`;
//     case "SparkLiteralExpression": return `lit(${x.value})`;
//     case "SparkGroupExpression": return `(${formatSparkExpression(x.value)})`;
//     case "SparkAppExpression": return `${x.func}(${x.args.map(formatSparkExpression).join(', ')})`;
//     case "SparkRawExpression": return x.value;
//   }
// }

// function justSparkModels(x: ModelDefinition[]): ModelDefinition[] {
//   return x.filter(x => x.modifiers.findIndex(x => x.kind === "TypeModelModifier") === -1);;
// }

// function makeScalaCaseClasses(models: ModelDefinition[]): [ScalaCaseClass[], ScalaCaseClass[], Map<string, string>] {
//   const classes = new Map<string, number>();
//   const mapping = new Map<string, string>();
//   let skip = false;
//   for (const [i, model] of models.entries()) {
//     const modelType = tc.typeCheck(model);
//     skip = false;
//     for (const [k, v] of classes) {
//       const type = tc.typeCheck(models[v]);
//       if (typeEquals(modelType, type)) {
//         mapping.set(model.name, k);
//         skip = true;
//         break;
//       }
//     }
//     if (!skip) {
//       classes.set(model.name, i);
//       // model is a spark model, aka not a type model
//       if (justSparkModels([model]).length === 1)
//         mapping.set(model.name, model.name);
//     }
//   }

//   const uniqueModels = [...classes.values()].map(x => models[x]);
//   const sparkModels = justSparkModels(uniqueModels);

//   return [
//     sparkModels.map(toScalaCaseClass),
//     uniqueModels.map(toScalaCaseClass),
//     mapping
//   ];
// }

// export function emit(m: Module): string {
//   const models = m.definitions.filter(is("ModelDefinition"));
//   const sparkModels = justSparkModels(models);
//   const indices = new Map(sparkModels.map((x, i) => [x.name, i]));


//   const [sparkTypes, allTypes, mapping] = makeScalaCaseClasses(models);
//   const deltaTypes = sparkTypes.map(addDeltaProperty);
//   const handlers = sparkModels.map(x => toSparkHandler(x, indices, mapping));

//   return scalaTemplate({
//     packageName: "Example",
//     names: [...mapping.values()],
//     allNames: sparkModels.map(x => x.name),
//     types: allTypes.map(formatScalaCaseClass),
//     deltaTypes: deltaTypes.map(formatScalaCaseClass),
//     handlers: handlers.map(formatSparkHandler),
//   });
// }
