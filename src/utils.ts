export function arraysEqual<T>(arr1: T[], arr2: T[]): boolean {
  if (arr1.length !== arr2.length) {
    return false;
  }

  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] !== arr2[i]) {
      return false;
    }
  }

  return true;
}

export class DaoError extends Error {}

export class DaoEntityNotFound extends DaoError {}

export class DaoInvalidUpdate extends DaoError {}

export class DaoAtomicCommitError extends DaoError {}
