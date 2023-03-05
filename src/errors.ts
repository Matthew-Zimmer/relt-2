
export function throws(msg: string): never {
  throw new InternalError(msg);
}

export class UserError extends Error { }
export class InternalError extends Error { }

export function assertExpectation(expect: boolean, msg?: string): asserts expect is true {
  if (!expect)
    throw new UserError(msg);
}

export function assertKind<T extends { kind: string }, K extends T['kind']>(x: T, kind: K, msg?: string): asserts x is T & { kind: K } {
  if (x.kind !== kind)
    throw new UserError(msg);
}

export function assertDefined<T>(x: T | undefined, msg?: string): asserts x is T {
  if (x === undefined)
    throw new UserError(msg);
}

export function assertUndefined<T>(x: T | undefined, msg?: string): asserts x is undefined {
  if (x !== undefined)
    throw new UserError(msg);
}

export function assertInvariant(expect: boolean, msg?: string): asserts expect is true {
  if (!expect)
    throw new InternalError(`Internal error, please report: ${msg ?? "No msg given :("}`);
}
