type NUMBER = unknown[];

type NOT<B extends boolean> = true extends B ? false : true;
type OR<L extends boolean, R extends boolean> = true extends L ? true : R;
type AND<L extends boolean, R extends boolean> = false extends L ? false : R;

type ZERO = [];
type SUC<T extends NUMBER> = [[], ...T];
type PREV<T extends NUMBER> = T extends [infer A, ...infer B] ? B : never;
type ADD<L extends NUMBER, R extends NUMBER> = L extends [infer H, ...infer T] ? ADD<T, SUC<R>> : R;
type SUB<L extends NUMBER, R extends NUMBER> = R extends [infer H, ...infer T] ? SUB<PREV<L>, T> : L;
type MUL<L extends NUMBER, R extends NUMBER> = L extends [infer H, ...infer T] ? ADD<R, MUL<T, R>> : ZERO;
type EXP<L extends NUMBER, R extends NUMBER> = L extends [infer H, ...infer T] ? MUL<R, EXP<T, R>> : ONE;

type EQ<L extends NUMBER, R extends NUMBER> = FROM<L> extends FROM<R> ? true : false;
type NEQ<L extends NUMBER, R extends NUMBER> = NOT<EQ<L, R>>;
type LT<L extends NUMBER, R extends NUMBER> =
  L extends [infer HL, ...infer TL] ?
  R extends [infer HR, ...infer TR] ? LT<TL, TR> : false
  : R extends [infer HR, ...infer TR] ? true : false;
type GT<L extends NUMBER, R extends NUMBER> = LT<R, L>;
type LTEQ<L extends NUMBER, R extends NUMBER> = NOT<GT<L, R>>;
type GTEQ<L extends NUMBER, R extends NUMBER> = NOT<LT<L, R>>;

type FROM<T extends NUMBER> = T['length'];
type TO<N extends number, L extends NUMBER = []> = L['length'] extends N ? L : TO<N, SUC<L>>;

type ONE = SUC<ZERO>;
type TWO = SUC<ONE>;
type THREE = SUC<TWO>;

function add<N extends number, M extends number>(x: N, y: M): FROM<ADD<TO<N>, TO<M>>> {
  return x + y as FROM<ADD<TO<N>, TO<M>>>;
}

function sub<N extends number, M extends number>(x: N, y: LTEQ<TO<M>, TO<N>> extends true ? M : never): FROM<SUB<TO<N>, TO<M>>> {
  return x - y as FROM<SUB<TO<N>, TO<M>>>;
}

function mul<N extends number, M extends number>(x: N, y: M): FROM<MUL<TO<N>, TO<M>>> {
  return x * y as FROM<MUL<TO<N>, TO<M>>>;
}

type HasDivisor<L extends NUMBER, R extends NUMBER, C extends NUMBER = L> =
  C extends [infer H, ...infer T] ? OR<EQ<MUL<C, R>, L>, HasDivisor<L, R, T>> : false;

function div<N extends number, M extends number>(x: N, y: HasDivisor<TO<N>, TO<M>> extends true ? M : never): number {
  return x / y;
}

function exp<N extends number, M extends number>(x: N, y: M): FROM<EXP<TO<N>, TO<M>>> {
  return x ** y as FROM<EXP<TO<N>, TO<M>>>;
}

const y = add(0, 0);
const z = sub(150, y);
const x = div(4, 3);

type REBUILD<T extends unknown[]> = T extends [infer A, ...infer B] ? [[], ...REBUILD<B>] : [];

type VectorData<T, N extends NUMBER> = N extends [infer H, ...infer B] ? [T, ...VectorData<T, B>] : [];

class Vector<T, N extends NUMBER> {
  constructor(public data: VectorData<T, N>) {
  }
}

function vector<T extends unknown[]>(...args: T): Vector<T extends (infer A)[] ? A : never, REBUILD<T>> {
  return new Vector(args as any);
}

function empty<T>(): Vector<T, ZERO> {
  return new Vector([]);
}

function push<T, N extends NUMBER, A extends unknown[]>(vector: Vector<T, N>, ...x: A): Vector<T, ADD<N, TO<FROM<A>>>> {
  return new Vector([...vector.data, ...x] as any);
}

function concat<T, N extends NUMBER, M extends NUMBER>(vector1: Vector<T, N>, vector2: Vector<T, M>): Vector<T, ADD<N, M>> {
  return new Vector([...vector1.data, ...vector2.data] as any);
}

function pop<T, N extends NUMBER>(vector: GT<N, ZERO> extends true ? Vector<T, N> : `Cannot pop from an empty vector`): [Vector<T, PREV<N>>, T] {
  const v = vector as Vector<T, N>;
  return [new Vector(v.data.slice(0, -1) as any as VectorData<T, PREV<N>>), v.data[v.data.length - 1]];
}

function idx<T, N extends NUMBER, I extends number>(vector: Vector<T, N>, idx: LT<TO<I>, N> extends true ? I : `${I} is out of bounds needs to be less than ${FROM<N>}`): T {
  return vector.data[idx as number];
}

function size<T, N extends NUMBER>(vector: Vector<T, N>): FROM<N> {
  return vector.data.length as any;
}

function reduce<T, N extends NUMBER, P = T>(vector: Vector<T, N>, f: (p: P, c: T) => P, initial: P): P;
function reduce<T, N extends NUMBER, P = T>(vector: GT<N, ZERO> extends true ? Vector<T, N> : `Cannot reduce an empty vector`, f: (p: P, c: T) => P, initial?: P): P;
function reduce<T, N extends NUMBER, P = T>(vector: Vector<T, N> | string, f: (p: P, c: T) => P, initial?: P): P {
  const v = vector as Vector<T, N>;
  return v.data.reduce(f, initial as any);
}


let v0 = empty<number>();
let v1 = push(v0, 1);
let [v2, _] = pop(v1);
let e = idx(v1, 0);
let v3 = concat(v1, v2);

const s = size(v3);

const q0 = reduce(v3, (p, c) => p + c);
const q1 = reduce(v2, (p, c) => p + c);
