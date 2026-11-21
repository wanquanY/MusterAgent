ALTER TABLE agent.owner_schedules
    ADD COLUMN pause_reason text,
    ADD CONSTRAINT owner_schedules_pause_reason_check CHECK (
        pause_reason IS NULL
        OR (
            status = 'paused'::text
            AND octet_length(pause_reason) >= 1
            AND octet_length(pause_reason) <= 1024
        )
    );
