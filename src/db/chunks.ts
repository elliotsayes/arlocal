import { Chunk } from 'faces/chunk';
import { Knex } from 'knex';
import { Utils } from '../utils/utils';

export class ChunkDB {
  private connection: Knex;

  constructor(connection: Knex) {
    this.connection = connection;
  }

  async create({
    chunk,
    data_root,
    data_size,
    chunk_size,
    local_offset,
    global_offset,
    data_path
  }: Chunk) {
    try {
      const id = Utils.randomID(64);

      await this.connection
        .insert({
          id: Utils.randomID(64),
          chunk,
          data_root,
          data_size,
          chunk_size,
          local_offset,
          global_offset,
          data_path,
        })
        .into('chunks');

      return id;
    } catch (error) {
      console.error({ error });
    }
  }

  async update(id: string, {
    chunk,
    data_root,
    data_size,
    chunk_size,
    local_offset,
    global_offset,
    data_path
  }: Partial<Chunk>) {
    try {
      await this.connection('chunks').where({ id })
        .update({
          chunk,
          data_root,
          data_size,
          chunk_size,
          local_offset,
          global_offset,
          data_path,
        });

      return id;
    } catch (error) {
      console.error({ error });
    }
  }

  async getByRootAndLocalOffset(dataRoot: string, localOffset: BigInt) {
    try {
      return (await this.connection('chunks').where({
        data_root: dataRoot,
        local_offset: localOffset.toString(),
      }))[0];
    } catch (error) {
      console.error({ error });
    }
  }

  async getRoot(dataRoot: string) {
    try {
      return await this.connection('chunks').where({ data_root: dataRoot });
    } catch (error) {
      console.error({ error });
    }
  }

  async getByGlobalOffset(global_offset: BigInt) {
    try {
      return (await this.connection('chunks').where({
        global_offset: global_offset.toString(),
      }))[0];
    } catch (error) {
      console.error({ error });
    }
  }

  async getCurrentGlobalOffset(): Promise<bigint> {
    try {
      const allChunks = await this.connection('chunks');
      let globalOffset = BigInt(0)
      for (const chunk of allChunks) {
        const x = BigInt(chunk.global_offset)
        if (x > globalOffset) {
          globalOffset = x;
        }
      }
      return globalOffset;
    } catch (error) {
      console.log('I crashed');
      console.error({ error });
    }
  }

  /**
   * An algorithm for finding and removing orphaned chunk.
   * (Could happen if a data item is uploaded multiple times with different size chunks.)
   * @param data_root string
   */
  async deleteOrphanedChunks(data_root: string) : Promise<void> {
    console.log(`begin: deleteOrphanedChunks('${data_root}')`)
    const chunks = await this.getRoot(data_root)
    const data_size = BigInt(chunks[0].data_size)
    const maxOffset = chunks.map(x => BigInt(x.global_offset)).reduce(
      (acc, x) => {
        return x > acc ? x : acc
      },
      BigInt(0),
    )
    console.log('maxOffset', maxOffset)
    
    const allChunkIds = chunks.map(x => x.id)
    const goodChunkIds = []

    let currentOffset = maxOffset
    let currentChunk;
    do {
      console.log('currentOffset', currentOffset)
      currentChunk = chunks.find(x => x.global_offset === currentOffset.toString())
      if (!currentChunk) {
        throw new Error('database error: chunk not found')
      }
      goodChunkIds.push(currentChunk.id)
      const chunk_size = BigInt(currentChunk.chunk_size)
      const newOffset = currentOffset - chunk_size
      currentOffset = newOffset
    } while (currentOffset > maxOffset - data_size)

    console.log('allChunkIds', allChunkIds)
    console.log('goodChunkIds', goodChunkIds)

    const deleteTransactions = []
    for (const chunkId of allChunkIds) {
      if (!goodChunkIds.includes(chunkId)) {
        deleteTransactions.push(
          this.connection('chunks').where({ id: chunkId }).del()
        )
      }
    }
    await Promise.all(deleteTransactions)
    console.log(`done: deleteOrphanedChunks('${data_root}')`)
  }

  static sort(chunks: Chunk[]) : void {
    chunks.sort((a, b) => {
      const bigIntA = BigInt(a.global_offset)
      const bigIntB = BigInt(b.global_offset)
      if (bigIntA == bigIntB) return 0
      return bigIntA > bigIntB ? 1 : -1
    })
  }
}
