DO $$
DECLARE baseline_id bigint; claimed_id bigint;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ops' AND table_name='sync_work_items' AND column_name='attempts_in_budget') THEN
    RAISE EXCEPTION 'Q03 hardening attempt budget column missing';
  END IF;
  SELECT id INTO baseline_id FROM ops.sync_work_items WHERE stable_key='q03-upgrade-stable';
  IF NOT EXISTS (SELECT 1 FROM ops.sync_work_items WHERE id=baseline_id
                 AND attempts=5 AND attempts_in_budget=5 AND status='quarantined') THEN
    RAISE EXCEPTION 'at-limit Q03 item was revived instead of remaining quarantined';
  END IF;
  IF NOT ops.retry_quarantined_repeatable_sync_work_item(baseline_id) THEN
    RAISE EXCEPTION 'explicit retry did not reopen exhausted Q03 item';
  END IF;
  SELECT id INTO claimed_id FROM ops.claim_next_repeatable_sync_work_item_with_lease('q03-upgrade-retry', interval '1 minute', 5);
  IF claimed_id IS DISTINCT FROM baseline_id THEN
    RAISE EXCEPTION 'explicit retry did not make exhausted Q03 item claimable';
  END IF;
END $$;
