/**
 * Close code passed to a Transport when a new connection is refused because the
 * admitted connection count is at its limit. SPEC 6.1 mandates `1013` (Try
 * Again Later) here; a non-WebSocket transport may ignore the value.
 *
 * Deliberately not shared with `SEND_BACKLOG_CLOSE_CODE` in connector-ws, which
 * carries the same number for the other side of admission: a connection the
 * layer already admitted whose outbound backlog is exhausted (SPEC 5.2). One
 * name for both triggers would invite changing one policy and silently moving
 * the other, so the duplication is intentional.
 */
export const CONNECTION_LIMIT_CLOSE_CODE = 1013;

/**
 * Close code passed to a Transport when the layer closes an admitted connection
 * because one of its event buffers reached its bound (SPEC 3.8).
 *
 * The same number as the other two 1013 triggers, for the reason above: an
 * admission refusal, a transport whose own send buffer is exhausted, and an
 * event backlog are three separate policies. Naming one for another would let a
 * change to one silently move the others.
 */
export const EVENT_BACKLOG_CLOSE_CODE = 1013;
