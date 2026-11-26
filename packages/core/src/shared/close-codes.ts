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
