import { up } from '../db/initialize';
import { readFile } from 'fs/promises';
import Router from 'koa-router';
import { BlockDB } from '../db/block';

export async function logsRoute(ctx: Router.RouterContext) {
  try {
    ctx.body = await readFile('./logs', 'utf-8');
    return;
  } catch (error) {
    console.error({ error });
  }
}

export async function resetRoute(ctx: Router.RouterContext) {
  try {
    const blockDB = new BlockDB(ctx.connection);
    await up(ctx.connection);
    const blockId = await blockDB.mineGenesisBlock();
    ctx.network.blocks = 1;
    ctx.network.current = blockId;
    ctx.network.height = 0;
    ctx.body = 'reset done';
  } catch (error) {
    console.error({ error });
  }
}
