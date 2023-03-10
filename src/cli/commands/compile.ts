import { spawnSync } from 'child_process';
import { compile as compileProject } from '../../compiler';
import { projectConfig } from '../../project';

export interface CompileArgs {
  toJar?: boolean;
  emit?: boolean;
}

export async function compile(args: CompileArgs): Promise<{ jarPath?: string }> {
  const toJar = args.toJar ?? false;
  const emit = args.emit ?? true;

  const project = await projectConfig();
  const { path } = await compileProject(project, { emit });

  if (toJar) {
    console.log("Compiling to Jar...");
    const res = spawnSync("sbt", ["assembly"], {
      cwd: "out"
    });
    if (res.status !== 0) {
      console.error(`Errors while compiling to jar. Please report!`);
      console.error(res.stderr.toString());
    }
  }

  return {
    jarPath: toJar ? path : undefined,
  };
}
