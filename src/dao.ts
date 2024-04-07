import { arraysEqual, DaoAtomicCommitError, DaoEntityNotFound, DaoError, DaoInvalidUpdate } from './utils.ts';

type KeyFuntion<T> = (entity: T) => unknown[];

type Index<T> = {
  key: KeyFuntion<T>;
  unique?: boolean;
};

type Options<T, IndexNames extends string> = {
  kv: Deno.Kv;
  name: string;
  primaryKey: KeyFuntion<T>;
  indices?: Record<IndexNames, Index<T>>;
};

type AtomicOption = {
  atomic?: Deno.AtomicOperation;
};

type Selector =
  | { prefix: unknown[] }
  | { prefix: unknown[]; start: unknown[] }
  | { prefix: unknown[]; end: unknown[] }
  | { start: unknown[]; end: unknown[] };

const PK = 'pk';
const COUNT = '__count__';

export abstract class KvDao<T, IndexNames extends string = never> {
  protected readonly kv: Deno.Kv;
  protected readonly name: string;
  protected readonly primaryKey: KeyFuntion<T>;
  protected readonly indices: Record<IndexNames, Index<T>>;

  constructor(options: Options<T, IndexNames>) {
    this.kv = options.kv;
    if (!options.name) throw new DaoError('invalid dao name');
    this.name = options.name;
    if (typeof options.primaryKey !== 'function') throw new DaoError('invalid primary key');
    this.primaryKey = options.primaryKey;
    this.indices = Object.freeze(options.indices ?? {} as Record<IndexNames, Index<T>>);
    for (const indexName of Object.keys(this.indices)) {
      const index = this.indices[indexName as IndexNames];
      if (indexName === PK) throw new DaoError(`index name '${PK}' invalid`);
      if (indexName === COUNT) throw new DaoError(`index name '${COUNT}' invalid`);
      if (typeof index.key !== 'function') throw new DaoError('invalid index key');
    }
  }

  private getKeyValue(keyFn: KeyFuntion<T>, entity: T): Deno.KvKeyPart[] {
    return keyFn(entity).map(serializeKeyPart);
  }

  private getIndexKeyValue(indexName: IndexNames, entity: T) {
    const key: Deno.KvKeyPart[] = [this.name, indexName];
    const index = this.indices[indexName];
    key.push(...this.getKeyValue(index.key, entity));
    if (!index.unique) {
      key.push(...this.getKeyValue(this.primaryKey, entity));
    }
    return key;
  }

  private withIndexCountKey(indexName: IndexNames, entity: T, cb: (key: Deno.KvKeyPart[]) => void) {
    const index = this.indices[indexName];
    if (index.unique) return; // unique index doesn't need count
    const key: Deno.KvKeyPart[] = [this.name, COUNT, indexName];
    const keyParts = this.getKeyValue(index.key, entity);
    for (const part of keyParts) {
      key.push(part);
      cb([...key]);
    }
  }

  private getPrimaryKeyValue(entity: T): Deno.KvKeyPart[] {
    return [this.name, PK, ...this.getKeyValue(this.primaryKey, entity)];
  }

  getPrimaryKey(entity: T): Deno.KvKeyPart[] {
    return this.getPrimaryKeyValue(entity);
  }

  private async withIndexes(cb: (indexName: IndexNames, index: Index<T>) => Promise<void> | void) {
    for (const indexName of Object.keys(this.indices)) {
      await cb(indexName as IndexNames, this.indices[indexName as IndexNames]);
    }
  }

  async create(entity: T, options: AtomicOption = {}) {
    const primaryKey = this.getPrimaryKeyValue(entity);

    const atomic = options.atomic ?? this.atomic();

    atomic.check({ key: primaryKey, versionstamp: null }).set(primaryKey, entity);
    atomic.sum([this.name, COUNT, PK], 1n);

    await this.withIndexes((name) => {
      const key = this.getIndexKeyValue(name, entity);
      atomic.set(key, entity);
      this.withIndexCountKey(name, entity, (key) => atomic.sum(key, 1n));
    });

    await this.commit(atomic, !!options.atomic);
  }

  async update(
    primaryKey: unknown[],
    updater: (current: T) => T,
    options: AtomicOption = {},
  ): Promise<T> {
    const key = [this.name, PK, ...primaryKey.map(serializeKeyPart)];

    const entry = await this.kv.get<T>(key);

    if (!entry.versionstamp) throw new DaoEntityNotFound('not_found: ' + primaryKey);
    const currentEntity = entry.value;
    const updatedEntity = updater(currentEntity);

    const updatedKey = this.getPrimaryKeyValue(updatedEntity);
    if (!arraysEqual(key, updatedKey)) throw new DaoInvalidUpdate('pk changed');

    const atomic = options.atomic ?? this.atomic();

    atomic.check(entry).set(entry.key, updatedEntity);

    await this.withIndexes((name) => {
      const key = this.getIndexKeyValue(name, updatedEntity);
      const currentKey = this.getIndexKeyValue(name, currentEntity);
      if (!arraysEqual(key, currentKey)) {
        atomic.delete(currentKey);
      }
      atomic.set(key, updatedEntity);

      this.updateIndexCounts(name, currentEntity, updatedEntity, atomic);
    });

    await this.commit(atomic, !!options.atomic);

    return updatedEntity;
  }

  private updateIndexCounts(name: IndexNames, currentEntity: T, updatedEntity: T, atomic: Deno.AtomicOperation) {
    const indexCountChanges = new Map<string, number>();
    this.withIndexCountKey(name, currentEntity, (key) => indexCountChanges.set(keyToJson(key), -1));
    this.withIndexCountKey(name, updatedEntity, (key) => {
      const keyJson = keyToJson(key);
      indexCountChanges.set(keyJson, (indexCountChanges.get(keyJson) ?? 0) + 1);
    });
    indexCountChanges.forEach((change, keyJson) => {
      if (change === 0) return;
      const key = keyFromJson(keyJson);
      atomic.sum(key, change < 0 ? 0xffffffffffffffffn : 1n);
    });
  }

  async delete(entity: T, options: AtomicOption = {}) {
    const atomic = options.atomic ?? this.atomic();
    atomic.delete(this.getPrimaryKeyValue(entity));
    atomic.sum([this.name, COUNT, PK], 0xffffffffffffffffn);

    await this.withIndexes((name) => {
      const key = this.getIndexKeyValue(name, entity);
      atomic.delete(key);
      this.withIndexCountKey(name, entity, (key) => atomic.sum(key, 0xffffffffffffffffn));
    });

    await this.commit(atomic, !!options.atomic);
  }

  async deleteByKey(primaryKey: unknown[], options: AtomicOption = {}): Promise<void> {
    const entity = await this.get(primaryKey);
    if (!entity) return;
    return this.delete(entity, options);
  }

  async get(primaryKey: unknown[]): Promise<T | null> {
    const kvKey = [this.name, PK, ...primaryKey.map(serializeKeyPart)];
    const entry = await this.kv.get<T>(kvKey);
    return entry.value;
  }

  async getByUniqueIndex(indexName: IndexNames, key: unknown[]): Promise<T | null> {
    if (!this.indices[indexName].unique) throw new DaoError('index not unique: ' + indexName);
    const kvKey = [this.name, indexName, ...key.map(serializeKeyPart)];
    const entry = await this.kv.get<T>(kvKey);
    return entry.value;
  }

  async listByIndex(indexName: IndexNames | typeof PK, selector?: Selector, options?: Deno.KvListOptions): Promise<[T[], string]> {
    const key = (parts: unknown[]) => [this.name, indexName, ...parts.map(serializeKeyPart)];
    const kvSelector = composeSelector(key, selector);
    const iter = this.kv.list<T>(kvSelector, options);
    const entities: T[] = [];
    for await (const res of iter) {
      entities.push(res.value);
    }
    return [entities, iter.cursor] as const;
  }

  async count(): Promise<bigint> {
    return (await this.kv.get<Deno.KvU64>([this.name, COUNT, PK])).value?.value ?? 0n;
  }

  async countByIndex(indexName: IndexNames, key: unknown[]): Promise<bigint> {
    const index = this.indices[indexName];
    if (index.unique) return (await this.getByUniqueIndex(indexName, key)) ? 1n : 0n;
    const countKey = [this.name, COUNT, indexName, ...key.map(serializeKeyPart)];
    return (await this.kv.get<Deno.KvU64>(countKey)).value?.value ?? 0n;
  }

  atomic(): Deno.AtomicOperation {
    return this.kv.atomic();
  }

  async commit(atomic: Deno.AtomicOperation, skip = false) {
    if (skip) return;
    const result = await atomic.commit();

    if (!result.ok) {
      throw new DaoAtomicCommitError('atomic commit error');
    }
  }
}

function composeSelector(key: (parts: unknown[]) => Deno.KvKeyPart[], selector?: Selector): Deno.KvListSelector {
  if (!selector) return { prefix: key([]) };
  if ('prefix' in selector) {
    if ('start' in selector) return { prefix: key(selector.prefix), start: key(selector.start) };
    if ('end' in selector) return { prefix: key(selector.prefix), end: key(selector.end) };
    return { prefix: key(selector.prefix) };
  }
  return { start: key(selector.start), end: key(selector.end) };
}

function serializeKeyPart(keyPart: unknown): Deno.KvKeyPart {
  if (
    typeof keyPart === 'string' ||
    typeof keyPart === 'number' ||
    typeof keyPart === 'boolean' ||
    typeof keyPart === 'bigint'
  ) {
    return keyPart;
  }

  if (keyPart instanceof Date) return keyPart.toISOString();

  if (keyPart === undefined) return '__undefined__';

  if (keyPart === null) return '__null__';

  if (typeof keyPart === 'object') {
    return JSON.stringify(keyPart);
  }

  throw new DaoError(`unserializable key part: ${keyPart} (type: ${typeof keyPart})`);
}

export function keyToJson(key: unknown[]): string {
  return JSON.stringify(key.map(serializeKeyPart), (_, keyPart) => {
    if (typeof keyPart === 'bigint') return `BigInt(${keyPart})`;
    return keyPart;
  });
}

export function keyFromJson(keyString: string): Deno.KvKeyPart[] {
  return JSON.parse(keyString, (_, keyPart) => {
    if (typeof keyPart === 'string' && keyPart.startsWith('BigInt(') && keyPart.endsWith(')')) {
      return BigInt(keyPart.slice(7, -1));
    }
    return keyPart;
  });
}
