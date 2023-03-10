import { Project } from "../project";
import { compileReltModule } from "./relt";
import { generateScalaProject } from "./spark";

export interface CompilerOptions {
  emit: boolean;
}

export async function compile(project: Project, options?: Partial<CompilerOptions>) {
  const emit = options?.emit ?? true;

  const module = await compileReltModule("src/main.relt");
  let path: string | undefined;
  if (emit) {
    path = await generateScalaProject(project, module);
  }

  return {
    path,
    module,
  };
}
