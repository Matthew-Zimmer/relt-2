import { ReltDefinition } from "./definition";

export interface ReltModule {
  kind: "ReltModule";
  definitions: ReltDefinition[];
}
