# Design and trade-off note

## Decisions
1. **SQLite over a managed warehouse:** zero credentials and reproducible local execution are appropriate for a 72-hour take-home vertical slice.
2. **Config-driven dataset contracts:** required fields, types, keys and SLAs are declared centrally to make the pipeline easier to extend.
3. **Generic target table:** storing payload JSON keeps the starter pipeline compact while preserving dataset identity and deterministic business keys.
4. **Quarantine rather than silent drops:** every rejected row includes the original values, source file, row number and reason.
5. **Append-only idempotent publish:** the composite primary key prevents repeated runs from duplicating business records.
6. **Separate operational metadata:** run history, per-dataset load events and rejected records are independently queryable.

## Known limitations
The starter does not implement updates/SCD2, streaming, distributed execution, email/Slack notifications, or full timezone-aware SLA calendars. The `unique` field in config is descriptive for this version; primary-key uniqueness is enforced. A schema mismatch fails the dataset/run rather than attempting unsafe automatic schema evolution.
