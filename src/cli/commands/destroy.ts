import { projectConfig } from "../../project";
import { adapters } from "../adapters";

export interface DestroyArgs {
  branch: string;
}

export async function destroy(args: DestroyArgs) {
  const { branch } = args;

  const project = await projectConfig();
  const { job, alert, cloud } = adapters(project, {
    op: "destroy",
    job: true,
    cloud: true,
  });

  await cloud.remove("");
  const state = await cloud.load(branch);
  await job.remove(state.job.id);
  await cloud.delete(branch);

  await alert?.msg(`Removed job for ${project.name}:${branch}`);
}
