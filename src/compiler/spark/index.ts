import { Project } from "../../project";
import { ReltModule } from "../relt/types";

export async function generateScalaProject(project: Project, reltModule: ReltModule): Promise<string> {
  return "target";
}
