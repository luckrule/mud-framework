/**
 * Close codes this transport sends. The numbers are WebSocket close codes, so a
 * non-WebSocket transport may map an equivalent constant to its own protocol's
 * equivalent semantics.
 *
 * Only `SEND_BACKLOG_CLOSE_CODE` is named by SPEC: SPEC 5.2 says an admitted
 * connection whose outbound backlog is exhausted MAY be closed with `1013`
 * (Try Again Later). `1011` and `1001` are this transport's choice: 1011 is
 * Internal Error for a failure the client cannot fix, 1001 Going Away when the
 * transport itself stops.
 *
 * `SEND_BACKLOG_CLOSE_CODE` duplicates core's `CONNECTION_LIMIT_CLOSE_CODE`
 * value on purpose: that one rejects a connection at admission (SPEC 6.1), this
 * one drops a connection the layer already admitted. Same number, different
 * trigger; one shared name would invite changing one policy and silently moving
 * the other.
 */
export const SEND_BACKLOG_CLOSE_CODE = 1013;

/** A transport handler threw or rejected while serving a connection. */
export const HANDLER_FAILED_CLOSE_CODE = 1011;

/**
 * A late `open`/`message` event for an id no longer registered. Kept apart from
 * `HANDLER_FAILED_CLOSE_CODE` although both use 1011: no handler failed here,
 * and the two causes may be re-prioritised independently.
 */
export const UNKNOWN_CONNECTION_CLOSE_CODE = 1011;

/** The transport is stopping and closes its live connections. */
export const TRANSPORT_STOPPED_CLOSE_CODE = 1001;
