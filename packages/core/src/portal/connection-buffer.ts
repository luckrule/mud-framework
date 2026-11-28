/** The slice of a connection's event queue the aggregate backlog tracks. */
export interface ConnectionBuffer {
  discardQueued(): { frames: number; bytes: number };
}
