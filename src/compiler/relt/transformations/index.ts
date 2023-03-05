import { ReltModule } from "../types";
import { noPipes } from "./noPipes";
import { reorder } from "./reorder";

export interface Transformation {
  name: string;
  transform: (x: ReltModule) => ReltModule;
}

export const transformations = {
  noPipes,
  reorder,
};
