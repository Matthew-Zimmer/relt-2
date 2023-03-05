import { throws } from "../../errors";
import { Project } from "../../project";
import { alertAdapters } from "./alert";
import { cloudAdapters } from "./cloud";
import { jobAdapters } from "./job";
import { runnerAdapters } from "./runner";

export interface JobAdapter {
  create: (options: { name: string, jarPath: string }) => Promise<{ id: string }>;
  run: (options: { id: string, parameters: string[] }) => Promise<void>;
  remove: (options: { id: string }) => Promise<void>;
}

export interface AlertAdapter {
  msg: (msg: string) => Promise<void>;
}

export interface CloudAdapter {
  upload: (from: string, to: string) => Promise<void>;
  remove: (path: string) => Promise<void>;
  save: (key: string, value: any) => Promise<void>;
  load: (key: string) => Promise<any>;
  delete: (key: string) => Promise<void>;
}

export interface RunnerAdapter {

}

export interface Adapters {
  job?: JobAdapter;
  alert?: AlertAdapter;
  cloud?: CloudAdapter;
  runner?: RunnerAdapter;
}

type RequiredAdapters<K extends Partial<Record<keyof Adapters, boolean>>> = {
  [P in keyof K & keyof Adapters as true extends K[P] ? P : never]: Exclude<Adapters[P], undefined>
} & Adapters;

export function adapters<K extends Partial<Record<keyof Adapters, boolean>>>(project: Project, required?: { op: string } & K): RequiredAdapters<K> {
  const keys = new Set(Object.keys(project));

  const createAdapter = <T>(kind: keyof Adapters, adapters: Record<string, (p: Project) => T>): T | undefined => {
    const results = Object.keys(adapters).filter(x => keys.has(x));
    if (results.length === 0) {
      if (required && required[kind] === true)
        throws(`A ${kind} adapter is required for this operation: ${required.op}`);
      return undefined;
    }
    if (results.length !== 1) throw new Error(`project config has multiple ${kind} adapters: ${results.join()}`);
    return adapters[results[0]](project);
  }

  const alert = createAdapter('alert', alertAdapters);
  const cloud = createAdapter('cloud', cloudAdapters);
  const job = createAdapter('job', jobAdapters);
  const runner = createAdapter('runner', runnerAdapters);

  return {
    alert,
    cloud,
    job,
    runner,
  } as RequiredAdapters<K>;
}
