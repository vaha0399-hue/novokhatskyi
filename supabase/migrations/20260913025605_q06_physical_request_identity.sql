-- Q06 follow-up: one spool request directory maps to exactly one source fetch.
BEGIN;

CREATE UNIQUE INDEX provider_fetches_q06_physical_request_uidx
    ON source.provider_fetches ((request_scope ->> 'physical_request_id'))
    WHERE request_scope ? 'physical_request_id';

COMMIT;
