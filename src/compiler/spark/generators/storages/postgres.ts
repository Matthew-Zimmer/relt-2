import { ReltModelDefinition } from "../../../relt/types";
import { ScalaDefExpression, ScalaObjectDefinition } from "../../types";
import { makeReadStorage, makeWriteStorage } from "./common";

export function makePostgresStorage(model: ReltModelDefinition): ScalaObjectDefinition {
  return {
    kind: "ScalaObjectDefinition",
    name: `${model.name}Storage`,
    properties: [
      makePostgresStorageRead(model),
      makePostgresStorageWrite(model),
    ]
  };
}

export function makePostgresStorageRead(model: ReltModelDefinition): ScalaDefExpression {
  return makeReadStorage(model.name, [
    {
      kind: "ScalaImportExpression",
      value: {
        kind: "ScalaDotExpression",
        left: {
          kind: "ScalaDotExpression",
          left: {
            kind: "ScalaIdentifierExpression",
            name: "spark"
          },
          right: {
            kind: "ScalaIdentifierExpression",
            name: "implicits"
          }
        },
        right: {
          kind: "ScalaIdentifierExpression",
          name: "_",
        }
      }
    },
    {
      kind: "ScalaReturnExpression",
      value: {
        kind: "ScalaDotExpression",
        hints: {
          indent: true,
        },
        left: {
          kind: "ScalaDotExpression",
          hints: {
            indent: true,
          },
          left: {
            kind: "ScalaDotExpression",
            hints: {
              indent: true,
            },
            left: {
              kind: "ScalaDotExpression",
              hints: {
                indent: true,
              },
              left: {
                kind: "ScalaDotExpression",
                hints: {
                  indent: true,
                },
                left: {
                  kind: "ScalaDotExpression",
                  hints: {
                    indent: true,
                  },
                  left: {
                    kind: "ScalaDotExpression",
                    hints: {
                      indent: true,
                    },
                    left: {
                      kind: "ScalaDotExpression",
                      hints: {
                        indent: true,
                      },
                      left: {
                        kind: "ScalaIdentifierExpression",
                        name: "spark"
                      },
                      right: {
                        kind: "ScalaIdentifierExpression",
                        name: "read"
                      },
                    },
                    right: {
                      kind: "ScalaAppExpression",
                      func: {
                        kind: "ScalaIdentifierExpression",
                        name: "format"
                      },
                      args: [{
                        kind: "ScalaStringExpression",
                        value: "jdbc"
                      }]
                    },
                  },
                  right: {
                    kind: "ScalaAppExpression",
                    func: {
                      kind: "ScalaIdentifierExpression",
                      name: "option"
                    },
                    args: [{
                      kind: "ScalaStringExpression",
                      value: "url"
                    }, {
                      kind: "ScalaStringExpression",
                      value: "jdbc:postgresql:dbserver"
                    }]
                  },
                },
                right: {
                  kind: "ScalaAppExpression",
                  func: {
                    kind: "ScalaIdentifierExpression",
                    name: "option"
                  },
                  args: [{
                    kind: "ScalaStringExpression",
                    value: "dbtable"
                  }, {
                    kind: "ScalaStringExpression",
                    value: "schema.tablename"
                  }]
                },
              },
              right: {
                kind: "ScalaAppExpression",
                func: {
                  kind: "ScalaIdentifierExpression",
                  name: "option"
                },
                args: [{
                  kind: "ScalaStringExpression",
                  value: "user"
                }, {
                  kind: "ScalaStringExpression",
                  value: "username"
                }]
              },
            },
            right: {
              kind: "ScalaAppExpression",
              func: {
                kind: "ScalaIdentifierExpression",
                name: "option"
              },
              args: [{
                kind: "ScalaStringExpression",
                value: "password"
              }, {
                kind: "ScalaStringExpression",
                value: "password"
              }]
            },
          },
          right: {
            kind: "ScalaAppExpression",
            func: {
              kind: "ScalaIdentifierExpression",
              name: "load"
            },
            args: []
          },
        },
        right: {
          kind: "ScalaIdentifierExpression",
          name: "as",
          types: [{
            kind: "ScalaDotType",
            left: {
              kind: "ScalaIdentifierType",
              name: "Types",
            },
            right: {
              kind: "ScalaIdentifierType",
              name: model.name,
            }
          }]
        }
      }
    }
  ]);
}

export function makePostgresStorageWrite(model: ReltModelDefinition): ScalaDefExpression {
  return makeWriteStorage(model.name, [{
    kind: "ScalaDotExpression",
    hints: {
      indent: true,
    },
    left: {
      kind: "ScalaDotExpression",
      hints: {
        indent: true,
      },
      left: {
        kind: "ScalaDotExpression",
        hints: {
          indent: true,
        },
        left: {
          kind: "ScalaDotExpression",
          hints: {
            indent: true,
          },
          left: {
            kind: "ScalaDotExpression",
            hints: {
              indent: true,
            },
            left: {
              kind: "ScalaDotExpression",
              hints: {
                indent: true,
              },
              left: {
                kind: "ScalaDotExpression",
                hints: {
                  indent: true,
                },
                left: {
                  kind: "ScalaIdentifierExpression",
                  name: "ds"
                },
                right: {
                  kind: "ScalaIdentifierExpression",
                  name: "write"
                },
              },
              right: {
                kind: "ScalaAppExpression",
                func: {
                  kind: "ScalaIdentifierExpression",
                  name: "format"
                },
                args: [{
                  kind: "ScalaStringExpression",
                  value: "jdbc"
                }]
              },
            },
            right: {
              kind: "ScalaAppExpression",
              func: {
                kind: "ScalaIdentifierExpression",
                name: "option"
              },
              args: [{
                kind: "ScalaStringExpression",
                value: "url"
              }, {
                kind: "ScalaStringExpression",
                value: "jdbc:postgresql:dbserver"
              }]
            },
          },
          right: {
            kind: "ScalaAppExpression",
            func: {
              kind: "ScalaIdentifierExpression",
              name: "option"
            },
            args: [{
              kind: "ScalaStringExpression",
              value: "dbtable"
            }, {
              kind: "ScalaStringExpression",
              value: "schema.tablename"
            }]
          },
        },
        right: {
          kind: "ScalaAppExpression",
          func: {
            kind: "ScalaIdentifierExpression",
            name: "option"
          },
          args: [{
            kind: "ScalaStringExpression",
            value: "user"
          }, {
            kind: "ScalaStringExpression",
            value: "username"
          }]
        },
      },
      right: {
        kind: "ScalaAppExpression",
        func: {
          kind: "ScalaIdentifierExpression",
          name: "option"
        },
        args: [{
          kind: "ScalaStringExpression",
          value: "password"
        }, {
          kind: "ScalaStringExpression",
          value: "password"
        }]
      },
    },
    right: {
      kind: "ScalaAppExpression",
      func: {
        kind: "ScalaIdentifierExpression",
        name: "save"
      },
      args: []
    },
  }]);
}
