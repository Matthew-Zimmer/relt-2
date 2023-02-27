import { generate } from 'peggy';

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
      "distinct", "where", "with",
      "union", "bool", "int", "string", "float",
      "false", "true", "join", "over", "or", "and"
      ].includes(chars[0] + chars[1].join('')) }
    { return chars[0] + chars[1].join('') }

  definition
    = model_definition

  module_definition
    = "mod" __ name: identifier
    { return { kind: "ModuleExpression", name } }

  model_definition
    = modifiers: (@model_modifier _)* _ "model" _ name: identifier _ "=" _ expression: expression
    { return { kind: "ModelDefinition", modifiers, name, expression } }

  model_modifier
    = delta_model_modifier

  delta_model_modifier
    = "delta" __ value: expression
    { return { kind: "DeltaModelModifier", value } }

  type
    = string_type
    / integer_type
    / float_type
    / boolean_type
    / identifier_type

  string_type
    = "string"
    { return { kind: "StringType" } }
  
  integer_type
    = "int"
    { return { kind: "IntegerType" } }
  
  float_type
    = "float"
    { return { kind: "FloatType" } }
  
  boolean_type
    = "bool"
    { return { kind: "BooleanType" } }

  identifier_type
    = name: identifier
    { return { kind: "identifierType", name } }

  expression
    = pipe_expression

  pipe_expression
    = ("|" _)? head: command_expression tail:(_ op: ("|") _ right: command_expression { return {
      kind: "PipeExpression",
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
    = head: (@below_command_expression _)? "where" _ condition: below_command_expression
    { return { kind: "WhereExpression", head: head ?? undefined, condition } }

  sort_expression
    = head: (@below_command_expression _)? "sort" _ columns: (h: identifier_expression t: (_ "," _ @identifier_expression)* { return [h, ...t] }) __ op: ("asc" / "desc")
    { return { kind: "SortExpression", head: head ?? undefined, columns, op } }

  over_expression
    = head: (@below_command_expression _)? "over" _ column: below_command_expression
    { return { kind: "OverExpression", head: head ?? undefined, column } }
  
  join_expression
    = head: (@below_command_expression _)? op: (@("left" / "right" / "inner") __)? "join" on: group_expression? _ other: below_command_expression
    { return { kind: "JoinExpression", head: head ?? undefined, op: op ?? "inner", on: on ?? undefined, other } }

  union_expression
    = head: (@below_command_expression _)? "union" _ other: below_command_expression
    { return { kind: "UnionExpression", head: head ?? undefined, other } }

  with_expression
    = head: (@below_command_expression _)? "with" _ "{" _ properties: (@object_property _ "," _)* _ "}"
    { return { kind: "WithExpression", head: head ?? undefined, properties } }

  object_property
    = name: identifier _ "=" _ value: expression
    { return { name, value } }

  below_command_expression
    = or_expression

  or_expression
    = head: and_expression tail:(_ op: ("or") _ right: and_expression { return {
      kind: 'OrExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  and_expression
    = head: cmp_expression tail:(_ op: ("and") _ right: cmp_expression { return {
      kind: 'AndExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  cmp_expression
    = head: add_expression tail:(_ op: ("==" / "!=" / "<=" / ">=" / "<" / ">") _ right: add_expression { return {
      kind: 'CmpExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  add_expression
    = head: mul_expression tail:(_ op: ("+" / "-") _ right: mul_expression { return {
      kind: 'AddExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  mul_expression
    = head: dot_expression tail:(_ op: ("*" / "/" / "%") _ right: dot_expression { return {
      kind: 'MulExpression',
      op,
      right,
    }})*
    { return tail.reduce((t, h) => ({ ...h, left: t }), head) }

  dot_expression
    = head: literal_expression tail:(_ op: (".") _ right: literal_expression { return {
      kind: 'DotExpression',
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
    { return { kind: "StringExpression", value: chars.join('') } }

  integer_expression
    = value: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") })
    { return { kind: "IntegerExpression", value: Number(value) } }

  float_expression
    = integer_part: ("0" / head: [1-9] tail: [0-9]* { return head + tail.join("") }) "." decimal_part: [0-9]+
    { return { kind: "FloatExpression", value: integer_part + "." + decimal_part.join("") } }

  boolean_expression
    = value: ("true" / "false")
    { return { kind: "BooleanExpression", value: value === "true" } }

  env_var_expression
    = "$\\"" chars: [^\\"]* "\\""
    { return { kind: "EnvVarExpression", value: chars.join('') } }

  identifier_expression
    = name: identifier
    { return { kind: "IdentifierExpression", name } }

  group_expression
    = "(" _ value: expression _ ")"
    { return { kind: "GroupExpression", value } }

  type_object_expression
    = "{" _ properties: (@type_object_property _ "," _)* _ "}"
    { return { kind: "TypeObjectExpression", properties } }

  type_object_property
    = name: identifier _ ":" _ type: type
    { return { name, type } }
`);
