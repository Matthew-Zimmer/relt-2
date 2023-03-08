import { compile as compileProject } from '../../compiler';
import { projectConfig } from '../../project';

export interface CompileArgs {
  toJar?: boolean;
}

export async function compile(args: CompileArgs): Promise<{ jarPath?: string }> {
  const project = await projectConfig();
  const { path } = await compileProject(project);

  return {
    jarPath: path,
  };
}
