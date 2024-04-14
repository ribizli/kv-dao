import { assert, assertFalse } from 'assert';
import { arraysEqual } from '../src/utils.ts';

Deno.test('arraysEqual', () => {
  assert(arraysEqual(['a', 'b', 1, 2], ['a', 'b', 1, 2]));
});

Deno.test('arraysEqual_different', () => {
  assertFalse(arraysEqual(['a', 'b', 1, 2], ['a', 'b']));
});
