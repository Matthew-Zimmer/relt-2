import { assertInvariant } from "../../../errors";
import { ReltModelDefinition } from "../types";
import { isStoredModel } from "./validInstructions";

export interface Vertex {
  hasStorage: boolean;
  parents: string[];
  children: string[];
}

export class DAG {
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

export const deps = new DAG();

export function vertexInfo(model: ReltModelDefinition): Vertex {
  const hasStorage = isStoredModel(model);
  const parents = deps.consumeFrom(model.name);
  const children = deps.feedsInto(model.name);

  return {
    hasStorage,
    parents,
    children,
  };
}
