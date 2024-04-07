import { assert, assertEquals, assertObjectMatch, assertRejects } from 'assert';
import { Dao, keyFromJson, keyToJson } from '../mod.ts';

type Person = {
  id: number;
  name: string;
  createdAt: Date;
  age: number;
};
class PersonDao extends Dao<Person, 'name' | 'createdAt' | 'age'> {
  constructor(kv: Deno.Kv) {
    super({
      kv,
      name: 'person',
      primaryKey: (entity) => [entity.id],
      indices: {
        name: {
          key: (entity) => [entity.name],
          unique: true,
        },
        createdAt: {
          key: (entity) => [entity.createdAt],
        },
        age: {
          key: (entity) => [entity.age, entity.name],
        },
      },
    });
  }
}

async function logDb(kv: Deno.Kv) {
  for await (const entry of kv.list({ prefix: [] })) {
    console.log(entry.key, entry.value);
  }
}

Deno.test('test DAO operation', async () => {
  const kv = await Deno.openKv(':memory:');
  try {
    const personDao = new PersonDao(kv);
    await personDao.create({
      id: 1,
      name: 'John',
      createdAt: new Date('2022-01-01T00:00:00.000Z'),
      age: 30,
    });

    await personDao.create({
      id: 2,
      name: 'Adam',
      createdAt: new Date('2023-01-01T00:00:00.000Z'),
      age: 30,
    });
    await personDao.create({
      id: 3,
      name: 'Frank',
      createdAt: new Date('2021-01-01T00:00:00.000Z'),
      age: 35,
    });

    logDb(kv);

    await personDao.count().then((count) => assertEquals(count, 3n));

    await personDao.get([1]).then((entity) =>
      assertObjectMatch(entity!, {
        id: 1,
        name: 'John',
        createdAt: new Date('2022-01-01T00:00:00.000Z'),
        age: 30,
      })
    );

    await personDao.getByUniqueIndex('name', ['Adam']).then((entity) => assertEquals(entity?.id, 2));

    await personDao.countByIndex('name', ['Adam']).then((count) => assertEquals(count, 1n));

    await personDao.countByIndex('age', [30]).then((count) => assertEquals(count, 2n));

    await personDao.listByIndex('age', { prefix: [] }).then(([list]) => {
      assertEquals(list.length, 3);
      assertEquals(list[0].id, 2);
      assertEquals(list[1].id, 1);
      assertEquals(list[2].id, 3);
    });

    await personDao.listByIndex('age', { prefix: [], start: [32] }).then(([list]) => {
      assertEquals(list.length, 1);
      assertEquals(list[0].id, 3);
    });

    await personDao.listByIndex('age', { prefix: [30] }).then(([list]) => {
      assertEquals(list.length, 2);
      assertEquals(list[0].id, 2);
      assertEquals(list[1].id, 1);
    });

    await personDao.listByIndex('age', { prefix: [] }, { limit: 1 }).then(([list, cursor]) => {
      assertEquals(list.length, 1);
      assertEquals(list[0].id, 2);
      return personDao.listByIndex('age', { prefix: [] }, { limit: 1, cursor }).then(([list, cursor]) => {
        assertEquals(list.length, 1);
        assertEquals(list[0].id, 1);
        return personDao.listByIndex('age', { prefix: [] }, { limit: 1, cursor }).then(([list]) => {
          assertEquals(list.length, 1);
          assertEquals(list[0].id, 3);
        });
      });
    });

    await personDao.update([1], (person) => ({ ...person, age: 40 }));

    logDb(kv);

    await personDao.get([1]).then((entity) => assertEquals(entity?.age, 40));

    await personDao.countByIndex('age', [30]).then((count) => assertEquals(count, 1n));

    await personDao.countByIndex('age', [40]).then((count) => assertEquals(count, 1n));

    await personDao.listByIndex('age', { prefix: [40] }).then(([list]) => {
      assertEquals(list.length, 1);
      assertEquals(list[0].id, 1);
    });

    await personDao.deleteByKey([2]);

    logDb(kv);

    await personDao.get([2]).then((entity) => assert(!entity));

    await personDao.count().then((count) => assertEquals(count, 2n));

    await personDao.countByIndex('age', [30]).then((count) => assertEquals(count, 0n));
  } finally {
    kv.close();
  }
});

Deno.test('keyToJson', () => {
  const keyString = keyToJson([1n, 2, '3', true, { test: 1, str: 's' }, new Date('2022-01-01T00:00:00.000Z'), undefined, null]);
  assertEquals(
    keyString,
    '["BigInt(1)",2,"3",true,"{\\"test\\":1,\\"str\\":\\"s\\"}","2022-01-01T00:00:00.000Z","__undefined__","__null__"]',
  );
  const key = keyFromJson(keyString);
  assertObjectMatch(key, {
    0: 1n,
    1: 2,
    2: '3',
    3: true,
    4: '{"test":1,"str":"s"}',
    5: '2022-01-01T00:00:00.000Z',
    6: '__undefined__',
    7: '__null__',
  });
});

Deno.test('atomic', async () => {
  const kv = await Deno.openKv(':memory:');
  try {
    const personDao = new PersonDao(kv);
    await personDao.create({
      id: 1,
      name: 'John',
      createdAt: new Date('2022-01-01T00:00:00.000Z'),
      age: 30,
    });
    await personDao.get([1]).then((entity) => {
      assertObjectMatch(entity!, {
        id: 1,
        name: 'John',
        createdAt: new Date('2022-01-01T00:00:00.000Z'),
        age: 30,
      });
    });

    await assertRejects(() =>
      Promise.all([
        personDao.update([1], (person) => ({ ...person, age: 31 })),
        personDao.update([1], (person) => ({ ...person, age: 32 })),
      ])
    );

    const atomic = personDao.atomic();
    await personDao.update([1], (person) => ({ ...person, age: 31 }), { atomic });
    await personDao.update([1], (person) => ({ ...person, age: 32 }), { atomic });

    await personDao.update([1], (person) => ({ ...person, age: 33 })).then((entity) => {
      assertEquals(entity?.age, 33);
    });

    await assertRejects(() => personDao.commit(atomic));

    await personDao.get([1]).then((entity) => {
      assertEquals(entity?.age, 33);
    });
  } finally {
    kv.close();
  }
});
