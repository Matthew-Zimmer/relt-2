"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.emit = exports.format = exports.pull = exports.applyToChildren = exports.fromChildren = exports.children = void 0;
const fs_1 = require("fs");
const promises_1 = require("fs/promises");
const grammar_1 = require("./grammar");
const stage0_1 = require("./stage0");
const ts_pattern_1 = require("ts-pattern");
function children(e) {
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
            return [e.left, e.right];
        case "WhereExpression": return e.head === undefined ? [e.condition] : [e.head, e.condition];
        case "SortExpression": return e.head === undefined ? e.columns : [e.head, ...e.columns];
        case "OverExpression": return e.head === undefined ? [e.column] : [e.head, e.column];
        case "UnionExpression": return e.head === undefined ? [e.other] : [e.head, e.other];
        case "JoinExpression": {
            if (e.head === undefined && e.on === undefined)
                return [e.other];
            else if (e.head === undefined)
                return [e.on, e.other];
            else if (e.on === undefined)
                return [e.head, e.other];
            else
                return [e.head, e.on, e.other];
        }
        case "WithExpression": {
            const fromProps = (x) => {
                switch (x.kind) {
                    case "AsObjectProperty": return [];
                    case "AssignObjectProperty":
                    case "OpAssignObjectProperty":
                    case "RenameObjectProperty":
                        return [x.value];
                }
            };
            return e.head === undefined ? e.properties.flatMap(fromProps) : [e.head, ...e.properties.flatMap(fromProps)];
        }
    }
}
exports.children = children;
function fromChildren(e, children) {
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
            return Object.assign(Object.assign({}, e), { value: children[0] });
        case "PipeExpression":
        case "OrExpression":
        case "AndExpression":
        case "CmpExpression":
        case "AddExpression":
        case "MulExpression":
        case "DotExpression":
        case "CoalesceExpression":
            return Object.assign(Object.assign({}, e), { left: children[0], right: children[1] });
        case "WhereExpression": return e.head === undefined ? Object.assign(Object.assign({}, e), { condition: children[0] }) : Object.assign(Object.assign({}, e), { head: children[0], condition: children[1] });
        case "SortExpression": return e.head === undefined ? Object.assign(Object.assign({}, e), { columns: children }) : Object.assign(Object.assign({}, e), { head: children[0], columns: children.slice(1) });
        case "OverExpression": return e.head === undefined ? Object.assign(Object.assign({}, e), { column: children[0] }) : Object.assign(Object.assign({}, e), { head: children[0], column: children[1] });
        case "UnionExpression": return e.head === undefined ? Object.assign(Object.assign({}, e), { other: children[0] }) : Object.assign(Object.assign({}, e), { head: children[0], other: children[1] });
        case "JoinExpression": {
            if (e.head === undefined && e.on === undefined)
                return Object.assign(Object.assign({}, e), { other: children[0] });
            else if (e.head === undefined)
                return Object.assign(Object.assign({}, e), { on: children[0], other: children[1] });
            else if (e.on === undefined)
                return Object.assign(Object.assign({}, e), { head: children[0], other: children[1] });
            else
                return Object.assign(Object.assign({}, e), { head: children[0], on: children[1], other: children[2] });
        }
        case "WithExpression": {
            const makeProps = (x, i) => {
                switch (x.kind) {
                    case "AsObjectProperty":
                        return x;
                    case "AssignObjectProperty":
                    case "OpAssignObjectProperty":
                    case "RenameObjectProperty":
                        return Object.assign(Object.assign({}, x), { value: children[i] });
                }
            };
            return e.head === undefined ? Object.assign(Object.assign({}, e), { properties: e.properties.map(makeProps) }) : Object.assign(Object.assign({}, e), { head: children[0], properties: e.properties.map(makeProps) });
        }
    }
}
exports.fromChildren = fromChildren;
function applyToChildren(e, f) {
    return fromChildren(e, children(e).map(f));
}
exports.applyToChildren = applyToChildren;
function pull(f, g) {
    return (x) => {
        let l = [];
        const h = (x) => {
            const [a, b] = f(x);
            l.push(...b);
            return a;
        };
        return [g(x, h), l];
    };
}
exports.pull = pull;
const end = (f, suffix) => (x) => f(x) + suffix;
const wrap = (c = "\"") => (x) => `${c}${x}${c}`;
const quote = wrap("\"");
const cap = (x) => x.length === 0 ? "" : `${x[0].toUpperCase()}${x.slice(1)}`;
const line = (f) => end(f, '\n');
const formatType = (t) => {
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
function format(m) {
    let indent = '';
    const conditionally = (x, f) => x === undefined ? '' : f(x);
    const withIndentation = (x, f) => {
        indent += "  ";
        const value = f(x);
        indent = indent.slice(0, -2);
        return value;
    };
    const formatDef = (d) => {
        switch (d.kind) {
            case "ModelDefinition":
                return `${d.modifiers.map(line(formatModelMod)).join('')}model ${d.name} = ${formatExpr(d.expression)}`;
        }
    };
    const formatModelMod = (x) => {
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
    const formatExpr = (e) => {
        switch (e.kind) {
            case "PipeExpression": {
                const fmt = (e) => `${formatExpr(e.left)}\n${indent}|${formatExpr(e.right)}`;
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
exports.format = format;
function applyTransformation(ast, transformation) {
    const nast = transformation.transform(ast);
    if (process.env.NODE_ENV === "development")
        (0, fs_1.writeFileSync)(`dev/${transformation.name}.relt`, format(nast));
    return nast;
}
class UserError extends Error {
}
class InternalError extends Error {
}
function assertExpectation(expect, msg) {
    if (!expect)
        throw new UserError(msg);
}
function assertInvariant(expect, msg) {
    if (!expect)
        throw new InternalError(`Internal error, please report: ${msg !== null && msg !== void 0 ? msg : "No msg given :("}`);
}
function conditionally(x, key, f) {
    return x[key] === undefined ? undefined : { [key]: f(x[key]) };
}
function is(kind) {
    return (x) => x.kind === kind;
}
const identity = {
    name: "identity",
    transform(m) {
        return m;
    }
};
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
        const def = (x) => {
            switch (x.kind) {
                case "ModelDefinition": {
                    const [e, d] = expr(x.expression);
                    return [...d, Object.assign(Object.assign({}, x), { expression: e })];
                }
                default:
                    return [x];
            }
        };
        const makeHead = (l, r) => {
            switch (r.kind) {
                case "WhereExpression":
                case "UnionExpression":
                case "OverExpression":
                case "JoinExpression":
                case "SortExpression":
                case "WithExpression":
                    return Object.assign(Object.assign({}, r), { head: l });
                default:
                    assertExpectation(false, `Cannot unpipe ${l.kind}`); /* hint for typescript */
                    throw '';
            }
        };
        const expr = (x) => {
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
                        };
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
};
class DAG {
    constructor() {
        this.feeds = new Map();
        this.consumes = new Map();
    }
    addVertex(v) {
        if (this.feeds.has(v) || this.consumes.has(v))
            assertInvariant(false, `${v} is already in the dag!`);
        this.feeds.set(v, []);
        this.consumes.set(v, []);
    }
    addEdge(start, end) {
        if (!this.feeds.has(start))
            assertInvariant(false, `Unknown vertex: ${start}`);
        if (!this.consumes.has(end))
            assertInvariant(false, `Unknown vertex: ${end}`);
        this.feeds.get(start).push(end);
        this.consumes.get(end).push(start);
    }
    sort() {
        return [];
    }
    feedsInto(x) {
        if (!this.feeds.has(x))
            assertInvariant(false, `unknown vertex: ${x}`);
        return this.feeds.get(x);
    }
    consumeFrom(x) {
        if (!this.consumes.has(x))
            assertInvariant(false, `unknown vertex: ${x}`);
        return this.consumes.get(x);
    }
}
const deps = new DAG();
const dag = {
    name: "dag",
    transform(x) {
        const def = (x) => {
            switch (x.kind) {
                case "ModelDefinition": {
                    deps.addVertex(x.name);
                    return x;
                }
            }
        };
        return { kind: "Module", definitions: x.definitions.map(def) };
    },
};
function typeEquals(l, r) {
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
                    if (l.properties.length !== r.properties.length)
                        return false;
                    const lP = new Map(l.properties.map(x => [x.name, x.type]));
                    const rP = new Map(r.properties.map(x => [x.name, x.type]));
                    if (l.properties.some(x => !rP.has(x.name)))
                        return false;
                    if (r.properties.some(x => !lP.has(x.name)))
                        return false;
                    return l.properties.every(x => typeEquals(lP.get(x.name), rP.get(x.name)));
                }
                default: return false;
            }
    }
}
function mergeStructs(l, r, options) {
    const { allowOverride } = options !== null && options !== void 0 ? options : { allowOverride: false };
    const properties = [...l.properties];
    for (const p of r.properties) {
        if (!allowOverride)
            assertExpectation(properties.findIndex(x => x.name === p.name) === -1, `${p.name} is already in struct`);
        properties.push(p);
    }
    return { kind: "StructType", properties };
}
const types = {
    bool: { kind: "BooleanType" },
    int: { kind: "IntegerType" },
    float: { kind: "FloatType" },
    date: { kind: "DateType" },
    string: { kind: "StringType" },
    opt: (of) => ({ kind: "OptionalType", of }),
};
function overload(args, params, res) {
    if (args.length !== params.length)
        return undefined;
    if (args.every((_, i) => typeEquals(args[i], params[i])))
        return res;
    return undefined;
}
function opTypeCheck(l, op, r) {
    switch (op) {
        case "??": {
            if (l.kind !== "OptionalType")
                return undefined;
            if (!typeEquals(l.of, r))
                return undefined;
            return l.of;
        }
        case "or": return (undefined
            || overload([l, r], [types.bool, types.int], types.bool)
            || overload([l, r], [types.opt(types.bool), types.opt(types.bool)], types.opt(types.bool))
            || overload([l, r], [types.opt(types.bool), types.bool], types.opt(types.bool))
            || overload([l, r], [types.bool, types.opt(types.bool)], types.opt(types.bool)));
        case "and": return (undefined
            || overload([l, r], [types.bool, types.int], types.bool)
            || overload([l, r], [types.opt(types.bool), types.opt(types.bool)], types.opt(types.bool))
            || overload([l, r], [types.opt(types.bool), types.bool], types.opt(types.bool))
            || overload([l, r], [types.bool, types.opt(types.bool)], types.opt(types.bool)));
        case "==": return (undefined
            || overload([l, r], [r, l], types.bool)
            || overload([l, r], [types.opt(r), l], types.opt(types.bool))
            || overload([l, r], [r, types.opt(l)], types.opt(types.bool))
            || overload([l, r], [types.opt(r), types.opt(l)], types.opt(types.bool)));
        case "!=": return (undefined
            || overload([l, r], [r, l], types.bool)
            || overload([l, r], [types.opt(r), l], types.opt(types.bool))
            || overload([l, r], [r, types.opt(l)], types.opt(types.bool))
            || overload([l, r], [types.opt(r), types.opt(l)], types.opt(types.bool)));
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
            || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool)));
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
            || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool)));
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
            || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool)));
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
            || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.bool)));
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
            || overload([l, r], [types.string, types.opt(types.string)], types.opt(types.string)));
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
            || overload([l, r], [types.date, types.opt(types.date)], types.opt(types.date)));
        case "*": return (undefined
            || overload([l, r], [types.int, types.int], types.int)
            || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
            || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
            || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))
            || overload([l, r], [types.float, types.float], types.float)
            || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
            || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
            || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float)));
        case "/": return (undefined
            || overload([l, r], [types.int, types.int], types.int)
            || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
            || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
            || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))
            || overload([l, r], [types.float, types.float], types.float)
            || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
            || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
            || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float)));
        case "%": return (undefined
            || overload([l, r], [types.int, types.int], types.int)
            || overload([l, r], [types.opt(types.int), types.opt(types.int)], types.opt(types.int))
            || overload([l, r], [types.opt(types.int), types.int], types.opt(types.int))
            || overload([l, r], [types.int, types.opt(types.int)], types.opt(types.int))
            || overload([l, r], [types.float, types.float], types.float)
            || overload([l, r], [types.opt(types.float), types.opt(types.float)], types.opt(types.float))
            || overload([l, r], [types.opt(types.float), types.float], types.opt(types.float))
            || overload([l, r], [types.float, types.opt(types.float)], types.opt(types.float)));
    }
}
class TypeChecker {
    constructor() {
        this.models = new Map();
        this.ctx = new Map();
    }
    typeCheck(x) {
        if (this.models.has(x.name))
            return this.models.get(x.name);
        const t = this.typeCheckExpr(x.expression);
        assertExpectation(is('StructType')(t), "");
        this.models.set(x.name, t);
        this.ctx.clear();
        return t;
    }
    typeCheckExpr(x) {
        switch (x.kind) {
            case "BooleanExpression": return types.bool;
            case "FloatExpression": return types.float;
            case "StringExpression": return types.string;
            case "EnvVarExpression": return types.string;
            case "IntegerExpression": return types.int;
            case "GroupExpression": return this.typeCheckExpr(x.value);
            case "IdentifierExpression": {
                if (this.models.has(x.name))
                    return this.models.get(x.name);
                assertExpectation(this.ctx.has(x.name), `Unknown variable ${x.name}`);
                return this.ctx.get(x.name);
            }
            case "TypeObjectExpression":
                return { kind: "StructType", properties: x.properties };
            case "DotExpression": {
                const l = this.typeCheckExpr(x.left);
                assertExpectation(is('StructType')(l), `Left side of a dot needs to be a struct type`);
                const oldCtx = new Map(this.ctx);
                this.ctx = new Map(l.properties.map(x => [x.name, x.type]));
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
                return t;
            }
            case "PipeExpression":
                assertInvariant(false, `Pipes should be removed`);
                throw '';
            case "JoinExpression": {
                assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
                const l = this.typeCheckExpr(x.head);
                assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
                const r = this.typeCheckExpr(x.other);
                assertExpectation(is('StructType')(r), `Right side of join needs to be a struct type`);
                return mergeStructs(l, r);
            }
            case "SortExpression": {
                assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
                const l = this.typeCheckExpr(x.head);
                assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
                const oldCtx = new Map(this.ctx);
                this.ctx = new Map(l.properties.map(x => [x.name, x.type]));
                x.columns.forEach(x => this.typeCheckExpr(x));
                this.ctx = oldCtx;
                return l;
            }
            case "WhereExpression": {
                assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
                const l = this.typeCheckExpr(x.head);
                assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
                const oldCtx = new Map(this.ctx);
                this.ctx = new Map(l.properties.map(x => [x.name, x.type]));
                const c = this.typeCheckExpr(x.condition);
                assertExpectation(typeEquals(c, types.bool), `Where condition needs to be a bool got ${formatType(c)}`);
                this.ctx = oldCtx;
                return l;
            }
            case "UnionExpression": {
                assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
                const l = this.typeCheckExpr(x.head);
                assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
                const r = this.typeCheckExpr(x.other);
                assertExpectation(is('StructType')(r), `Right side of join needs to be a struct type`);
                assertExpectation(typeEquals(l, r), `Cannot union non equal struct types`);
                return l;
            }
            case "WithExpression":
                assertInvariant(x.head !== undefined, `Heads should be set from pipe rewrites`);
                const l = this.typeCheckExpr(x.head);
                assertExpectation(is('StructType')(l), `Left side of join needs to be a struct type`);
                const properties = x.properties.map(x => {
                    switch (x.kind) {
                        case "AsObjectProperty":
                            return { name: x.name, type: x.type };
                        case "AssignObjectProperty":
                        case "RenameObjectProperty":
                        case "OpAssignObjectProperty":
                            return { name: x.name, type: this.typeCheckExpr(x.value) };
                    }
                });
                return mergeStructs(l, { kind: "StructType", properties }, { allowOverride: true });
            case "OverExpression":
                throw `TODO`;
        }
    }
}
const tc = new TypeChecker();
function toScalaCaseClass(x) {
    const t = tc.typeCheck(x);
    return { name: x.name, properties: t.properties.map(x => ({ name: x.name, type: toScalaType(x.type) })) };
}
function toScalaType(x) {
    switch (x.kind) {
        case "BooleanType": return { kind: "ScalaBooleanType" };
        case "FloatType": return { kind: "ScalaDoubleType" };
        case "IntegerType": return { kind: "ScalaIntType" };
        case "StringType": return { kind: "ScalaStringType" };
        case "StructType":
            assertInvariant(false, 'Should not be able to convert a struct type to scala');
            throw '';
        case "IdentifierType": return { kind: "ScalaDotType", left: { kind: "ScalaIdentifierType", name: "Types" }, right: { kind: "ScalaIdentifierType", name: x.name } };
        case "JsonType": return toScalaType(x.of);
        case "DateType": return { kind: "ScalaDateType" };
        case "ArrayType": return { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Array" }, of: toScalaType(x.of) };
        case "OptionalType": return { kind: "ScalaOfType", type: { kind: "ScalaIdentifierType", name: "Option" }, of: toScalaType(x.of) };
    }
}
function createStorage(x) {
    let storage = undefined;
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
function isSourceType(x) {
    if (x.modifiers.some(x => x.kind === "TypeModelModifier"))
        return false;
    const expr = (x) => {
        switch (x.kind) {
            case "TypeObjectExpression": return true;
            default: return children(x).some(expr);
        }
    };
    return expr(x.expression);
}
function makePlans(x, isSource, hasCache) {
    const mods = new Set(x.modifiers.map(x => x.kind));
    if (mods.has('TypeModelModifier'))
        return {};
    const hasInvalidate = !isSource && hasCache && !mods.has('IndexModelModifier');
    const hasStore = hasCache;
    const hasFetch = isSource || hasCache;
    const hasRefresh = isSource;
    const hasDerive = !isSource;
    return Object.assign(Object.assign(Object.assign(Object.assign(Object.assign({}, !hasInvalidate ? {} : {
        invalidatePlan: { args: ["storage"], typeArgs: ['DeltaTypes', 'Types'] },
    }), !hasStore ? {} : {
        storePlan: { args: ["storage", "create", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
    }), !hasFetch ? {} : {
        fetchPlan: { args: ["storage", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
    }), !hasRefresh ? {} : {
        refreshPlan: { args: ["storage", "combine"], typeArgs: ['DeltaTypes', 'Types'] },
    }), !hasDerive ? {} : {
        derivePlan: { args: ["create", "combine"], typeArgs: ['DeltaTypes'] }
    });
}
function makeCreateFunction(x, indices, mapping) {
    switch (x.expression.kind) {
        case "JoinExpression":
            return {
                kind: "SparkJoinFunction",
                lIdx: indices.get(x.expression.head.name),
                on: x.expression.on === undefined ? (() => { assertInvariant(false, `Cannot make join without on condition`); throw ''; })() : makeSparkExpression(x.expression.on),
                rIdx: indices.get(x.expression.other.name),
                type: x.expression.op,
                typeName: mapping.get(x.name),
            };
        case "SortExpression":
            return {
                kind: "SparkSortFunction",
                idx: indices.get(x.expression.head.name),
                columns: x.expression.columns.map(x => x.name),
                type: "asc"
            };
        case "UnionExpression":
            return {
                kind: "SparkUnionFunction",
                lIdx: indices.get(x.expression.head.name),
                rIdx: indices.get(x.expression.other.name),
            };
        case "WhereExpression":
            return {
                kind: "SparkWhereFunction",
                idx: indices.get(x.expression.head.name),
                cond: makeSparkExpression(x.expression.condition),
            };
        case "WithExpression": {
            const oldType = tc.typeCheck(x);
            return {
                kind: "SparkWithColumnFunction",
                idx: indices.get(x.expression.head.name),
                properties: x.expression.properties.map(x => {
                    switch (x.kind) {
                        case "AsObjectProperty": {
                            const oldTy = oldType.properties.find(y => y.name === x.name).type;
                            return { name: x.name, rename: false, value: makeCastSparkExpression(x.name, oldTy, x.type) };
                        }
                        case "AssignObjectProperty":
                            return { name: x.name, rename: false, value: makeSparkExpression(x.value) };
                        case "OpAssignObjectProperty":
                            return { name: x.name, rename: false, value: makeOpSparkExpression(x.op, x.value) };
                        case "RenameObjectProperty":
                            return { name: x.name, rename: true, value: makeSparkExpression(x.value) };
                    }
                }),
                typeName: mapping.get(x.name),
            };
        }
        case "OverExpression":
            assertInvariant(false, `TODO`);
            throw '';
        default:
            assertInvariant(false, `Cannot create a creation function for model which does not have a table expression`);
            throw '';
    }
}
function makeSparkExpression(x) {
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
            assertInvariant(false, `Cannot convert ${x.kind} to spark expression`);
            throw '';
    }
}
function makeOpSparkExpression(op, x) {
    return (0, ts_pattern_1.match)([op, x])
        .with(["??=", { kind: "AndExpression" }], ([, r]) => {
        return { kind: "SparkLiteralExpression", value: "TODO" };
    })
        .with(ts_pattern_1.P._, () => { throw new InternalError(``); })
        .exhaustive();
}
function cast(name, typeName) {
    return {
        kind: "SparkBinaryExpression",
        left: {
            kind: "SparkColumnExpression",
            value: name,
        },
        op: ".",
        right: {
            kind: "SparkAppExpression",
            func: "cast",
            args: [{
                    kind: "SparkRawExpression",
                    value: typeName
                }]
        },
    };
}
function makeCastSparkExpression(name, oldType, type) {
    return (0, ts_pattern_1.match)([oldType, type])
        .with([{ kind: "StringType" }, { kind: "StringType" }], ([l, r]) => {
        console.warn(`Casting string to string`);
        return { kind: "SparkColumnExpression", value: name };
    })
        .with([{ kind: "StringType" }, { kind: "IntegerType" }], ([l, r]) => {
        return cast(name, "IntType");
    })
        .with([{ kind: "StringType" }, { kind: "FloatType" }], ([l, r]) => {
        return cast(name, "FloatType");
    })
        .with([{ kind: "StringType" }, { kind: "OptionalType", of: { kind: "StringType" } }], ([l, r]) => {
        return { kind: "SparkColumnExpression", value: name };
    })
        .with([{ kind: "IntegerType" }, { kind: "StringType" }], ([l, r]) => {
        return cast(name, "StringType");
    })
        .with([{ kind: "IntegerType" }, { kind: "FloatType" }], ([l, r]) => {
        return cast(name, "FloatType");
    })
        .with([{ kind: "IntegerType" }, { kind: "IntegerType" }], ([l, r]) => {
        console.warn(`Casting integer to integer`);
        return { kind: "SparkColumnExpression", value: name };
    })
        .with([{ kind: "IntegerType" }, { kind: "OptionalType", of: { kind: "IntegerType" } }], ([l, r]) => {
        return { kind: "SparkColumnExpression", value: name };
    })
        .with([{ kind: "FloatType" }, { kind: "StringType" }], ([l, r]) => {
        return cast(name, "StringType");
    })
        .with([{ kind: "FloatType" }, { kind: "FloatType" }], ([l, r]) => {
        console.warn(`Casting float to float`);
        return { kind: "SparkColumnExpression", value: name };
    })
        .with([{ kind: "FloatType" }, { kind: "IntegerType" }], ([l, r]) => {
        return cast(name, "IntType");
    })
        .with([{ kind: "FloatType" }, { kind: "OptionalType", of: { kind: "FloatType" } }], ([l, r]) => {
        return { kind: "SparkColumnExpression", value: name };
    })
        .with(ts_pattern_1.P._, ([l, r]) => { throw new InternalError(`Cannot cast ${formatType(l)} to ${formatType(r)}`); })
        .exhaustive();
}
function toSparkHandler(x, indices, mapping) {
    const idx = indices.get(x.name);
    const count = mapping.size;
    const storage = createStorage(x);
    const isSource = isSourceType(x);
    const hasCache = storage !== undefined && !isSource;
    const typeName = mapping.get(x.name);
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
function addDeltaProperty(x) {
    return Object.assign(Object.assign({}, x), { properties: [
            ...x.properties,
            {
                name: "__delta_state_kind",
                type: {
                    kind: "ScalaDotType",
                    left: { kind: "ScalaIdentifierType", name: "DeltaState" },
                    right: { kind: "ScalaIdentifierType", name: "Kind" }
                }
            },
        ] });
}
function formatScalaType(x) {
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
function formatScalaCaseClass(x) {
    return `case class ${x.name} (\n${x.properties.map(end(x => `  ${x.name}: ${formatScalaType(x.type)}`, ',\n')).join('')})`;
}
function formatSparkHandler(x) {
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
        `private val storage = new ${x.storage.name}[Types.${x.typeName}](${x.storage.args.join(', ')})`}

    ${Object.entries(x.plans).map(([k, v]) => `private val ${k} = new ${cap(k)}[DeltaTypes.Datasets, ${v.typeArgs.map(a => `${a}.${x.typeName}`)}](${v.args.map(x => `this.${x}`).join(',')})\n`).join('')}

    val name = "${x.name}"
    val consumes = Array[String](${x.consumes.map(quote).join(', ')})
    val feeds = Array[String](${x.consumes.map(quote).join(', ')})
    val hasCache = ${x.hasCache}
    val plans = Map(
      ${Object.keys(x.plans).map(k => `${quote(cap(k))} -> this.${k},\n`).join('')}
    )
  }`;
}
function formatSparkTableFunction(x) {
    switch (x.kind) {
        case "SparkJoinFunction": return `dss._${x.lIdx + 1}.join(dss._${x.rIdx + 1}, ${formatSparkExpression(x.on)}, "${x.type}").as[DeltaTypes.${x.typeName}]`; // try without as
        case "SparkSortFunction": return `dss._${x.idx + 1}.orderBy(${x.columns.map(c => `col("${c}").${x.type}`)})`; // try without as
        case "SparkUnionFunction": return `dss._${x.lIdx + 1}.union(dss._${x.rIdx + 1})`; // try without as
        case "SparkWhereFunction": return `dss._${x.idx + 1}.where(${formatSparkExpression(x.cond)})`; // try without as  
        case "SparkWithColumnFunction": return `dss._${x.idx + 1}${x.properties.map(x => `.withColumn("${x.name}", ${formatSparkExpression(x.value)})`)}.as[DeltaTypes.${x.typeName}]`;
    }
}
function formatSparkExpression(x) {
    switch (x.kind) {
        case "SparkBinaryExpression": return `(${formatSparkExpression(x.left)} ${x.op} ${formatSparkExpression(x.right)})`;
        case "SparkColumnExpression": return `col("${x.value}")`;
        case "SparkLiteralExpression": return `lit(${x.value})`;
        case "SparkGroupExpression": return `(${formatSparkExpression(x.value)})`;
        case "SparkAppExpression": return `${x.func}(${x.args.map(formatSparkExpression).join(', ')})`;
        case "SparkRawExpression": return x.value;
    }
}
function justSparkModels(x) {
    return x.filter(x => x.modifiers.findIndex(x => x.kind === "TypeModelModifier") === -1);
    ;
}
function makeScalaCaseClasses(models) {
    const classes = new Map();
    const mapping = new Map();
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
function emit(m) {
    const models = m.definitions.filter(is("ModelDefinition"));
    const sparkModels = justSparkModels(models);
    const indices = new Map(sparkModels.map((x, i) => [x.name, i]));
    const [sparkTypes, allTypes, mapping] = makeScalaCaseClasses(models);
    const deltaTypes = sparkTypes.map(addDeltaProperty);
    const handlers = sparkModels.map(x => toSparkHandler(x, indices, mapping));
    return (0, stage0_1.scalaTemplate)({
        packageName: "Example",
        names: [...mapping.values()],
        allNames: sparkModels.map(x => x.name),
        types: allTypes.map(formatScalaCaseClass),
        deltaTypes: deltaTypes.map(formatScalaCaseClass),
        handlers: handlers.map(formatSparkHandler),
    });
}
exports.emit = emit;
class Ast {
    constructor(module) {
        this.module = module;
    }
    apply(t) {
        this.module = applyTransformation(this.module, t);
        return this;
    }
    emit() {
        return emit(this.module);
    }
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        const fileContent = (yield (0, promises_1.readFile)('main.relt')).toString();
        let ast = new Ast(grammar_1.parser.parse(fileContent));
        // console.log(inspect(ast, false, null, true));
        const scala = ast
            .apply(identity)
            .apply(noPipes)
            .apply(dag)
            .emit();
        yield (0, promises_1.writeFile)('target/src/main/scala/out.scala', scala);
    });
}
main();
