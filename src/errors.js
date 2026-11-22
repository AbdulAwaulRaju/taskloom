export class TaskloomError extends Error {
  constructor(message, code) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
  }
}

/** The caller sent something invalid (maps to HTTP 400). */
export class ValidationError extends TaskloomError {
  constructor(message) {
    super(message, 'VALIDATION');
  }
}

/** The referenced job does not exist (maps to HTTP 404). */
export class NotFoundError extends TaskloomError {
  constructor(message) {
    super(message, 'NOT_FOUND');
  }
}

/** The operation conflicts with current state, e.g. a lost lease (maps to HTTP 409). */
export class ConflictError extends TaskloomError {
  constructor(message) {
    super(message, 'CONFLICT');
  }
}

/** Raised by the write-ahead log when the file is damaged in the middle. */
export class CorruptLogError extends TaskloomError {
  constructor(message) {
    super(message, 'CORRUPT_LOG');
  }
}
