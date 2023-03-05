import { Transformation } from ".";
import { assertInvariant } from "../../../errors";
import { ReltDefinition } from "../types";

class DAG {
  private feeds = new Map<string, string[]>();
  private consumes = new Map<string, string[]>();

  addVertex(v: string) {
    if (this.feeds.has(v) || this.consumes.has(v))
      assertInvariant(false, `${v} is already in the dag!`);
    this.feeds.set(v, []);
    this.consumes.set(v, []);
  }

  addEdge(start: string, end: string) {
    if (!this.feeds.has(start))
      assertInvariant(false, `Unknown vertex: ${start}`);
    if (!this.consumes.has(end))
      assertInvariant(false, `Unknown vertex: ${end}`);
    this.feeds.get(start)!.push(end);
    this.consumes.get(end)!.push(start);
  }

  sort(): string[] {
    return [];
  }

  feedsInto(x: string): string[] {
    if (!this.feeds.has(x))
      assertInvariant(false, `unknown vertex: ${x}`);
    return this.feeds.get(x)!;
  }

  consumeFrom(x: string): string[] {
    if (!this.consumes.has(x))
      assertInvariant(false, `unknown vertex: ${x}`);
    return this.consumes.get(x)!;
  }
}

const deps = new DAG();

export const reorder = {
  name: "reorder",
  transform(x) {
    const def = (x: ReltDefinition): ReltDefinition => {
      switch (x.kind) {
        case "ReltModelDefinition": {
          deps.addVertex(x.name);
          return x;
        }
      }
    };
    return { kind: "ReltModule", definitions: x.definitions.map(def) };
  },
} satisfies Transformation;