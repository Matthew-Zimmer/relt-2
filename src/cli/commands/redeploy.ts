import { projectConfig } from "../../project";
import { adapters } from "../adapters";
import { compile } from "./compile";

export interface RedeployArgs {
  branch: string;
}

export async function redeploy(args: RedeployArgs) {
  const { branch } = args;
  const project = await projectConfig();
  const { job, alert, cloud } = adapters(project, {
    op: "redeploy",
    job: true,
    cloud: true,
  });

  const { jarPath } = await compile({ toJar: true });
  await cloud.upload(jarPath!, "");
  const state = await cloud.load(branch);
  await job.run({
    id: state.job.id,
    parameters: [],
  });

  await alert?.msg(`Updated job for ${project.name}:${branch}`);
}
