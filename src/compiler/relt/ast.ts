import { writeFileSync } from "fs";
import { formatRelt } from "./format";
import { Transformation } from "./transformations";
import { ReltModule } from "./types";

export class Ast {
  constructor(public module: ReltModule) {
  }

  apply(transformation: Transformation): Ast {
    this.module = transformation.transform(this.module);
    if (process.env.NODE_ENV === "development")
      writeFileSync(`dev/${transformation.name}.relt`, formatRelt(this.module));
    return this;
  }
}
