import { existsSync } from "fs";
import { writeFile, rm, mkdir, copyFile } from "fs/promises";
import { Project } from "../../project";
import { getValue, isExternalModel } from "../relt/analysis/validInstructions";
import { ReltExternalModelModifier, ReltModelDefinition, ReltModule } from "../relt/types";
import { makeSparkEntryPoint } from "./generators";

export async function generateScalaProject(project: Project, reltModule: ReltModule): Promise<string> {
  const models = reltModule.definitions;
  await ensureCleanDirectoryExists(`out`);
  await ensureCleanDirectoryExists(`out/project`);
  await ensureCleanDirectoryExists(`out/src/main/scala`);

  await writeFile("out/build.sbt", makeBuildScript(project));
  await writeFile("out/project/build.properties", makeBuildProperties(project));
  await writeFile("out/src/main/scala/main.scala", makeSparkEntryPoint(project, models));
  await writeFile("out/project/plugins.sbt", makePluginsScript(project));

  await copyExternalSources(models);

  return `out/target/scala-2.12/${project.name}.jar`;
}

export function makeBuildScript(project: Project): string {
  return `\
name := "${project.name}"

version := "0.0.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.0",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % "3.0.1",
  "org.postgresql" % "postgresql" % "42.5.4",
)

assembly / assemblyMergeStrategy := {   
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard   
  case x => MergeStrategy.first 
}

assembly / assemblyJarName := "${project.name}.jar"
`;
}

export function makePluginsScript(project: Project): string {
  return `\
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
`;
}

export function makeBuildProperties(project: Project): string {
  return `sbt.version = 1.8.0\n`;
}

export async function ensureCleanDirectoryExists(path: string) {
  if (existsSync(path))
    await rm(path, { recursive: true });
  return mkdir(path, { recursive: true });
}


export async function copyExternalSources(models: ReltModelDefinition[]) {
  return Promise.all(models.filter(isExternalModel).map(copyExternalSource));
}

export async function copyExternalSource(model: ReltModelDefinition) {
  const mod = model.modifiers.find(x => x.kind === "ReltExternalModelModifier") as ReltExternalModelModifier;

  const path = getValue(mod.value).replace(/\./g, '/');
  const targetPath = `out/src/main/scala/${path}`;

  await ensureCleanDirectoryExists(targetPath.split('/').slice(0, -1).join('/'));

  return copyFile(`src/${path}.scala`, `${targetPath}.scala`);
}
