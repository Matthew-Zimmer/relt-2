import { writeFile } from "fs/promises";
import { Project } from "../../project";
import { ReltModule } from "../relt/types";
import { makeProject, toScala } from "./generators";

export async function generateScalaProject(project: Project, reltModule: ReltModule): Promise<string> {
  const scala = toScala(project, reltModule.definitions);

  await writeFile("out/main.scala", scala);

  return "target";
}
