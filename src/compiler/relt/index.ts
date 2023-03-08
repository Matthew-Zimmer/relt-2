import { parseRelt } from './grammar';
import { transformations } from './transformations';
import { ReltModule } from './types';

export async function compileReltModule(fileName: string): Promise<ReltModule> {
  const ast = await parseRelt(fileName);

  return (ast
    .apply(transformations.reorder)
    .module
  );
}
