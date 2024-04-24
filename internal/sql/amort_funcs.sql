create schema if not exists revenue_tracker;

create or replace function revenue_tracker.round_half_even(rval numeric, rlimit int default 7)
returns numeric
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
as $$
DECLARE
   result numeric;
BEGIN
    IF rlimit < 0 THEN
        RAISE EXCEPTION 'The rounding limit must be >= 0';
    END IF;

    rval := rval * 10 ^ rlimit;
    result := round(rval);

    IF trunc(rval) != result AND trunc(rval) % 2 = 0 THEN
        result := trunc(rval);
    END IF;

    RETURN result / 10 ^ rlimit;
END;
$$;

drop function if exists revenue_tracker.amortize_recognition_rate(numeric, int, int);
create or replace function revenue_tracker.amortize_recognition_rate(cash_value numeric, day_count int, rounding_limit int default 7)
returns table (
    daily_recognition_amount numeric,
    rounding_error numeric
)
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
as $$
DECLARE
    rounding_error numeric;
    daily_amount numeric;
BEGIN
    IF day_count > 1 THEN
        daily_amount := revenue_tracker.round_half_even(cash_value / day_count::numeric, rounding_limit);
        rounding_error := cash_value - daily_amount * day_count;
    ELSE -- covers both last day and immediate day recognition
        daily_amount := 0;
    END IF;

    return query
        select daily_amount, rounding_error;
END;
$$;

create table if not exists revenue_tracker.revenue_event (
    service_id uuid not null,
    valid_from_ts timestamptz not null,
    valid_to_ts timestamptz not null,

    customer_id uuid not null,
    event_id uuid not null,
    created timestamptz not null,
    paid timestamptz not null,
    currency_code text not null CHECK (currency_code ~ '[A-Z]{3}'),  -- e.g. USD, CAD, GBP
    amount bigint not null CHECK(amount > 0),
    term_start_dt timestamptz not null,
    term_end_dt timestamptz not null,

    revenue_ts daterange not null,
    revenue_days int not null,
    revenue_amount_daily numeric not null,
    revenue_amount_rounding numeric not null,

    primary key (service_id, valid_to_ts),
    unique (service_id, valid_from_ts)
);

create or replace function revenue_tracker.revenue_event_type_2() returns trigger as $$
BEGIN
    ------------------------------------------
    -- Handle all txns BEFORE they are written
    ------------------------------------------
    IF TG_OP = 'INSERT' AND TG_WHEN = 'BEFORE' THEN
        if NEW.valid_from_ts is null THEN
            NEW.valid_from_ts := clock_timestamp();
        END IF;
        -- Force inserts to be infinity records
        NEW.valid_to_ts = 'Infinity';

        if NEW.event_id is null THEN
            NEW.event_id := uuid_generate_v1();
        end if;

        NEW.revenue_ts := ('['||NEW.term_start_dt::date||','||NEW.term_end_dt::date||')')::tstzrange;
        NEW.revenue_days := NEW.term_end_dt::date - NEW.term_start_dt::date;

        select
            daily_recognition_amount, rounding_error into NEW.revenue_amount_daily, NEW.revenue_amount_rounding
        from
            revenue_tracker.amortize_recognition_rate(NEW.amount::numeric, NEW.revenue_days);

        update revenue_tracker.revenue_event set
            valid_to_ts = NEW.valid_from_ts
        where
            valid_to_ts = 'Infinity'
          and valid_from_ts < NEW.valid_from_ts
          and service_id = NEW.service_id;

        -- Return record
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' AND TG_WHEN = 'BEFORE' THEN
        RAISE EXCEPTION '% in % on %.% has been dsiabled', TG_OP, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        return NULL;

    ELSIF TG_OP = 'DELETE' AND TG_WHEN = 'BEFORE' THEN
        -- Throw out _all_ delete events
        RETURN NULL;

    ELSIF TG_OP = 'TRUNCATE' AND TG_WHEN = 'BEFORE' THEN
        RAISE EXCEPTION '% in % on %.% has been dsiabled', TG_OP, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;

        ------------------------------------------
        -- Handle all txns AFTER the write
        ------------------------------------------
    ELSIF TG_OP = 'INSERT' AND TG_WHEN = 'AFTER' THEN
        RETURN NULL;

    ELSIF TG_OP = 'UPDATE' AND TG_WHEN = 'AFTER' THEN
        RAISE EXCEPTION '% in % on %.% has been dsiabled', TG_OP, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN NULL;

    ELSIF TG_OP = 'DELETE' AND TG_WHEN = 'AFTER' THEN
        RETURN NULL;

    ELSE
        RAISE EXCEPTION 'Unhandled transaction type % in %.% on %.%', TG_OP, TG_WHEN, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;

    END IF;
END;
$$ LANGUAGE plpgsql;

drop trigger if exists MM_revenue_event_noop on revenue_tracker.revenue_event;
create trigger MM_revenue_event_noop
    before update
    on revenue_tracker.revenue_event
    for each row
execute procedure suppress_redundant_updates_trigger();

drop trigger if exists NM_revenue_event_type_2 on revenue_tracker.revenue_event;
create trigger NM_revenue_event_type_2
    before insert or update or delete
    on revenue_tracker.revenue_event
    for each row
    when (pg_trigger_depth() = 0)
execute procedure revenue_tracker.revenue_event_type_2();

drop trigger if exists OM_revenue_event_type_2 on revenue_tracker.revenue_event;
create trigger OM_revenue_event_type_2
    after insert or update or delete
    on revenue_tracker.revenue_event
    for each row
    when (pg_trigger_depth() = 0)
execute procedure revenue_tracker.revenue_event_type_2();

-- truncate table revenue_tracker.revenue_event;
--
-- insert into revenue_tracker.revenue_event (service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)
-- values ('8aa1bbf2-0273-11ef-8c4d-98fa9b5e176f', '28ca482c-0278-11ef-8c4d-98fa9b5e176f', 'USD', 14400, '2021-01-01 09:53:16.543', '2022-01-01 09:53:16.543', '2021-01-01', '2021-01-01', '2021-01-01');
-- insert into revenue_tracker.revenue_event (service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)
-- values ('8aa1bbf2-0273-11ef-8c4d-98fa9b5e176f', '28ca482c-0278-11ef-8c4d-98fa9b5e176f', 'USD', 38400, '2021-03-15 09:53:16.543', '2022-01-01 09:53:16.543', '2021-03-15 08:00', '2021-03-15 08:00', '2021-03-15 08:00');
-- insert into revenue_tracker.revenue_event (service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)
-- values ('8aa1bbf2-0273-11ef-8c4d-98fa9b5e176f', '28ca482c-0278-11ef-8c4d-98fa9b5e176f', 'USD', 11520, '2021-03-15 09:53:16.543', '2022-01-01 09:53:16.543', '2021-03-15 09:00', '2021-03-15 09:00', '2021-03-15 09:00');
--
-- insert into revenue_tracker.revenue_event (service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)
-- values ('9021a650-0273-11ef-8c4d-98fa9b5e176f', '535f39ee-0278-11ef-8c4d-98fa9b5e176f', 'USD', 14400, '2021-01-01 09:53:16.543', '2022-01-01 09:53:16.543', '2021-01-01', '2021-01-01', '2021-01-01');
-- insert into revenue_tracker.revenue_event (service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)
-- values ('9021a650-0273-11ef-8c4d-98fa9b5e176f', '535f39ee-0278-11ef-8c4d-98fa9b5e176f', 'USD', 38400, '2021-03-15 09:53:16.543', '2022-01-01 09:53:16.543', '2021-03-15 08:00', '2021-03-15 08:00', '2021-03-15 08:00');
--
-- insert into revenue_tracker.revenue_event (service_id, customer_id, currency_code, amount, term_start_dt, term_end_dt, valid_from_ts, created, paid)
-- values ('938a439c-0273-11ef-8c4d-98fa9b5e176f', '6152979e-0278-11ef-8c4d-98fa9b5e176f', 'USD', 14400, '2021-01-01 09:53:16.543', '2022-01-01 09:53:16.543', '2021-01-01', '2021-01-01', '2021-01-01');
