import { readFile } from "fs/promises";

export type Project = {
  name: string;
} & Record<string, any>;

export async function projectConfig() {
  return JSON.parse((await readFile("reltconfig.json")).toString()) as Project;
}
