import { Project } from "../project";
import { compileReltModule } from "./relt";
import { generateScalaProject } from "./spark";

export async function compile(project: Project) {
  const module = await compileReltModule("src/main.relt");
  const path = await generateScalaProject(project, module);
  return {
    path
  };
}
