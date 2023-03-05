import { ScalaDefinition } from "./definition";

export interface ScalaModule {
  kind: "ScalaModule";
  definitions: ScalaDefinition[];
}
