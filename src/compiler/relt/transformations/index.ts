import { ReltModule } from "../types";
import { reorder } from "./reorder";

export interface Transformation {
  name: string;
  transform: (x: ReltModule) => ReltModule;
}

export const transformations = {
  reorder,
};
