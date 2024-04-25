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
    amount bigint not null CHECK(amount > 0),  -- amount in the smallest denomination of the currency (e.g. cents)
    term_start_dt timestamptz not null,
    term_end_dt timestamptz not null,

    revenue_ts daterange not null,
    revenue_days int not null,
    revenue_amount_daily numeric not null,
    revenue_amount_rounding numeric not null,

    primary key (service_id, valid_from_ts)
);

create unique index if not exists re_latest_event_idx on revenue_tracker.revenue_event (service_id) where valid_to_ts = 'Infinity';

-- this is the index that we would use for finding out the revenue over a range of dates at a given point in time when we knew it
-- a gist index may work best here for time range overlaps
create index if not exists re_term_valid on revenue_tracker.revenue_event (lower(revenue_ts), upper(revenue_ts), valid_from_ts) include (valid_to_ts, revenue_ts);

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

-- create or replace function revenue_tracker.calculate_revenue
-- (
--     revenue_range daterange,
--     cutoff_time timestamptz
-- )
-- returns table
-- (
--     revenue_range_start_dt date,
--     revenue_range_end_dt date,
--     recognized_amount numeric
-- )
-- LANGUAGE plpgsql
-- PARALLEL SAFE
-- as $$
-- DECLARE
--     final_amount numeric;
-- BEGIN
--     return query
--         select daily_amount, final_amount;
-- END;
-- $$;

create or replace function revenue_tracker.calculate_event_revenue
(
    event revenue_tracker.revenue_event,
    next_event_start_dt date,
    revenue_query_range daterange
)
returns table
(
    revenue_range daterange,
    recognized_amount numeric
)
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
as $$
DECLARE
    event_effective_range daterange;
    num_days int;
    rounding_amount numeric default 0.0;
BEGIN
    IF next_event_start_dt is not null then
        event_effective_range := ('['||lower(event.revenue_ts)||','||least(next_event_start_dt, upper(event.revenue_ts))||')')::daterange;
    else
        event_effective_range := event.revenue_ts;
    end if;
    revenue_range := event_effective_range * revenue_query_range;
    num_days := upper(revenue_range) - lower(revenue_range);
    if upper(revenue_range) = upper(event.revenue_ts) then
        rounding_amount := event.revenue_amount_rounding;
    end if;

    return query
        select revenue_range, (event.revenue_amount_daily * num_days) + rounding_amount;
END;
$$;

-- select
--     r.*,
--     lower(rf.revenue_ts) as cutoff_dt,
--     rev.*
-- from
--     revenue_tracker.revenue_event r
--         left join revenue_tracker.revenue_event rf on
--         rf.service_id = r.service_id
--             and rf.valid_from_ts = r.valid_to_ts
--         cross join revenue_tracker.calculate_event_revenue(
--             event => r,
--             next_event_start_dt => lower(rf.revenue_ts),
--             revenue_query_range => '[2024-05-01,2024-06-1)'::daterange
--                    ) as rev
-- where
--     r.event_id = '3c1212d0-0281-11ef-8c4d-98fa9b5e176f'
-- ;
-- -- in the query below the date range is 5/1 to 6/1 (non inclusive) with a pov of 2024-05-15
-- select
--     t1.service_id,
--     t1.revenue_amount_daily,
--     ('['||greatest('2024-05-01', lower(t1.revenue_ts))||', '||least('2024-06-01', coalesce(lower(t2.revenue_ts), upper(t1.revenue_ts)))||')')::daterange as revenue_range
--
-- from
--     revenue_tracker.revenue_event t1
--         left join revenue_tracker.revenue_event t2 on
--         t1.service_id = t2.service_id
--             and t1.valid_to_ts = t2.valid_from_ts
--             and t2.valid_from_ts < '2024-05-15'
-- where
--     (
--         (
--             lower(t1.revenue_ts) <= '2024-05-01'
--                 and upper(t1.revenue_ts) > '2024-05-01'
--             ) or
--         (
--             lower(t1.revenue_ts) < '2024-06-01'
--                 and upper(t1.revenue_ts) >= '2024-06-01'
--             ) or
--         (
--             lower(t1.revenue_ts) > '2024-05-01'
--                 and upper(t1.revenue_ts) <= '2024-06-01'
--             )
--         )
--   and t1.valid_from_ts < '2024-05-15'
