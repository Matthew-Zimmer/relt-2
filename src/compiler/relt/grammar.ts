import { readFile } from 'fs/promises';
import { generate } from 'peggy';
import { ReltModule } from './types';
import { Ast } from './ast';

export const parser = generate(`
  module 
    = _ definitions: (@definition _)*
    { return { kind: "Module", definitions } }

  // optional whitespace
  _  = [ \\t\\r\\n]*

  // mandatory whitespace
  __ = [ \\t\\r\\n]+

  identifier 
    = chars: ([a-zA-Z][a-zA-Z0-9_]*)
    ! { return [
      "model", "fk", "pk", "sort", 
      "distinct", "where", "with", "on",
      "union", "bool", "int", "string", "float",
      "false", "true", "join", "over", "or", "and", "date"
      ].includes(chars[0] + chars[1].join('')) }
    { return chars[0] + chars[1].join('') }

  definition
    = model_definition

  module_definition
    = "mod" __ name: identifier
    { return { kind: "ReltModule", name } }

  model_definition
    = modifiers: (@model_modifier _)* _ "model" _ name: identifier _ "=" _ expression: expression
    { return { kind: "ReltModelDefinition", modifiers, name, expression } }

  model_modifier
    = delta_model_modifier
    / postgres_model_modifier
    / index_model_modifier
    / type_model_modifier

  delta_model_modifier
    = "delta" __ value: (string_expression / env_var_expression)
    { return { kind: "ReltDeltaModelModifier", value } }

  postgres_model_modifier
    = "postgres" __ value: (string_expression / env_var_expression)
    { return { kind: "ReltPostgresModelModifier", value } }

  index_model_modifier
    = "index" __ value: (string_expression / env_var_expression) __ "on" __ on: string_expression
    { return { kind: "ReltIndexModelModifier", value, on } }

  type_model_modifier
    = "type"
    { return { kind: "ReltTypeModelModifier" } }

  type
    = postfix_type

  postfix_type
    = head: primitive_type tail:(_ op: ("[]" / "?" / "json") { return {
      kind: {
        "[]": "ArrayType",
        "?": "OptionalType",
        "json": "JsonType",
      }[op],
    }})*
    { return tail.reduce((t, h) => ({ ...h, of: t }), head) }
    
  primitive_type
    = string_type
    / integer_type
    / float_type
    / boolean_type
    / identifier_type
    / date_type

  string_type
    = "string"
    { return { kind: "ReltStringType" } }
  
  integer_type
    = "int"
    { return { kind: "ReltIntegerType" } }
  
  float_type
    = "float"
    { return { kind: "ReltFloatType" } }
  
  boolean_type
    = "bool"
    { return { kind: "ReltBooleanType" } }

  identifier_type
    = name: identifier
    { return { kind: "ReltIdentifierType", name } }

  date_type
    = "date" fmt: (_ @string_expression)?
    { return { kind: "ReltDateType", fmt: fmt === null ? undefined : fmt.value } }

  expression
    = pipe_expression

  pipe_expression
    = ("|" _)? head: command_expression tail:(_ op: ("|") _ right: command_expression { return {
      kind: "ReltPipeExpression",
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  command_expression
    = where_expression
    / sort_expression
    / over_expression
    / join_expression
    / union_expression
    / with_expression
    / below_command_expression
  
  where_expression
    = "where" _ condition: below_command_expression
    { return { kind: "ReltWhereExpression", condition } }

  sort_expression
    = "sort" _ columns: (h: identifier_expression t: (_ "," _ @identifier_expression)* { return [h, ...t] }) __ op: ("asc" / "desc")
    { return { kind: "ReltSortExpression", columns, op } }

  over_expression
    = "over" _ column: below_command_expression
    { return { kind: "ReltOverExpression", column } }
  
  join_expression
    = op: (@("left" / "right" / "inner") __)? "join" on: group_expression? _ other: below_command_expression
    { return { kind: "ReltJoinExpression", op: op ?? "inner", on: on ?? undefined, other } }

  union_expression
    = "union" _ other: below_command_expression
    { return { kind: "ReltUnionExpression", other } }

  with_expression
    = "with" _ "{" _ properties: (@object_property _ "," _)* _ "}"
    { return { kind: "ReltWithExpression", properties } }

  object_property
    = assign_object_property
    / as_object_property
    / op_assign_object_property
    / rename_object_property

  assign_object_property
    = name: identifier _ "=" _ value: expression
    { return { kind: "ReltAssignObjectProperty", name, value } }

  as_object_property
    = name: identifier _ "as" _ type: type
    { return { kind: "ReltAsObjectProperty", name, type } }

  op_assign_object_property
    = name: identifier _ op: ("??=") _ value: expression
    { return { kind: "ReltOpAssignObjectProperty", name, op, value } }

  rename_object_property
    = name: identifier _ ":=" _ value: expression
    { return { kind: "ReltRenameObjectProperty", name, value } }

  below_command_expression
    = coalesce_expression

  coalesce_expression
    = head: or_expression tail:(_ op: ("??") _ right: or_expression { return {
      kind: 'ReltCoalesceExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  or_expression
    = head: and_expression tail:(_ op: ("or") _ right: and_expression { return {
      kind: 'ReltOrExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  and_expression
    = head: cmp_expression tail:(_ op: ("and") _ right: cmp_expression { return {
      kind: 'ReltAndExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  cmp_expression
    = head: add_expression tail:(_ op: ("==" / "!=" / "<=" / ">=" / "<" / ">") _ right: add_expression { return {
      kind: 'ReltCmpExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  add_expression
    = head: mul_expression tail:(_ op: ("+" / "-") _ right: mul_expression { return {
      kind: 'ReltAddExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  mul_expression
    = head: dot_expression tail:(_ op: ("*" / "/" / "%") _ right: dot_expression { return {
      kind: 'ReltMulExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  dot_expression
    = head: literal_expression tail:(_ op: (".") _ right: literal_expression { return {
      kind: 'ReltDotExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  literal_expression
    = string_expression
    / float_expression
    / integer_expression
    / boolean_expression
    / env_var_expression
    / identifier_expression
    / group_expression
    / type_object_expression

  string_expression
    = "\\"" chars: [^\\"]* "\\""
    { return { kind: "ReltStringExpression", value: chars.join('') } }

  integer_expression
    = value: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") })
    { return { kind: "ReltIntegerExpression", value: Number(value) } }

  float_expression
    = integer_part: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") }) "." decimal_part: [0-9]+
    { return { kind: "ReltFloatExpression", value: integer_part + "." + decimal_part.join("") } }

  boolean_expression
    = value: ("true" / "false")
    { return { kind: "BooleanExpression", value: value === "true" } }

  env_var_expression
    = "$\\"" chars: [^\\"]* "\\""
    { return { kind: "ReltEnvVarExpression", value: chars.join('') } }

  identifier_expression
    = name: identifier
    { return { kind: "ReltIdentifierExpression", name } }

  group_expression
    = "(" _ value: expression _ ")"
    { return { kind: "ReltGroupExpression", value } }

  type_object_expression
    = "{" _ properties: (@type_object_property _ "," _)* _ "}"
    { return { kind: "ReltTypeObjectExpression", properties } }

  type_object_property
    = name: identifier _ ":" _ type: type
    { return { name, type } }
`);

export async function parseRelt(filename: string): Promise<Ast> {
  const cnt = (await readFile(filename)).toString();
  const module = parser.parse(cnt) as ReltModule;
  return new Ast(module);
}
