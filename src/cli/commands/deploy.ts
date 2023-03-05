import { projectConfig } from "../../project";
import { adapters } from "../adapters";
import { compile } from "./compile";

export interface DeployArgs {
  branch: string;
}

export async function deploy(args: DeployArgs) {
  const { branch } = args;

  const project = await projectConfig();
  const { job, alert, cloud } = adapters(project, {
    op: "deploy",
    job: true,
    cloud: true,
  });

  const { jarPath } = compile({ toJar: true });
  await cloud.upload(jarPath!, "");
  const newJob = await job.create({
    jarPath: jarPath!,
    name: project.name,
  });
  await job.run({
    id: newJob.id,
    parameters: [],
  });
  await cloud.save(branch, { job: { id: newJob.id } });

  await alert?.msg(`Created job for ${project.name}:${branch}`);
}
