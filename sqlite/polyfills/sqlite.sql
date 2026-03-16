-- https://github.com/2ndQuadrant/mysqlcompat/blob/3b5319eed968ca32504d15cdd64ad0837b18929e/sql_bits/string.sql#L308

CREATE OR REPLACE FUNCTION public.json_type(col anyelement)
RETURNS text AS $$
	select json_typeof(col::json)
$$
LANGUAGE sql
;

CREATE OR REPLACE FUNCTION public.json_valid(_txt anyelement)
RETURNS bool
LANGUAGE plpgsql IMMUTABLE STRICT AS
$func$
	BEGIN
		RETURN _txt::json IS NOT NULL;
	EXCEPTION
		WHEN SQLSTATE '22P02' THEN  -- invalid_text_representation
			RETURN false;
	END
$func$
;

-- better compatibility because sqlite can manage json arrays, with the key as the item index
CREATE OR REPLACE FUNCTION public.sqlite_json_each(col json)
RETURNS table (key text, value text)
LANGUAGE plpgsql IMMUTABLE STRICT AS
$func$
	BEGIN
		if json_typeof(col) = 'array' THEN
			RETURN QUERY SELECT (row_number() over() - 1)::text as key, a.value::text as value from json_array_elements(col) a;
		ELSE
			RETURN QUERY SELECT a.key, a.value::text as value from json_each(col) a;
		END IF;
	END
$func$
;

-- https://dba.stackexchange.com/a/22513
CREATE OR REPLACE FUNCTION public.randomblob(p_length in integer) returns bytea language plpgsql IMMUTABLE as $$
declare
  o bytea := '';
begin 
  for i in 1..p_length loop
    o := o||decode(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0'), 'hex');
  end loop;
  return o;
end;$$
;

CREATE OR REPLACE FUNCTION public.hex(bytea)
RETURNS text AS $$
  select encode($1, 'hex')
$$ IMMUTABLE STRICT LANGUAGE SQL;
;

CREATE COLLATION IF NOT EXISTS NOCASE (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
;

CREATE OR REPLACE FUNCTION public.bool_int_equality (bool, int)
  RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 10
RETURN $1::int = $2
;

CREATE OR REPLACE FUNCTION public.bool_int_inequality (bool, int)
  RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 10
RETURN $1::int != $2
;

create or replace procedure public.create_bool_int_equality_operators()
  language plpgsql
as $$
begin
    execute $create$
		CREATE OPERATOR public.= (
		LEFTARG    = bool
		, RIGHTARG   = int
		, FUNCTION   = public.bool_int_equality
		, COMMUTATOR = OPERATOR(public.=)
		, NEGATOR    = OPERATOR(public.!=)
		)
		;

		CREATE OPERATOR public.!= (
		LEFTARG    = bool
		, RIGHTARG   = int
		, FUNCTION   = public.bool_int_inequality
		, COMMUTATOR = OPERATOR(public.!=)
		, NEGATOR    = OPERATOR(public.=)
		);
		$create$; 
    exception 
       when sqlstate '42723' then null; 
end;
$$;

CALL create_bool_int_equality_operators();

CREATE OR REPLACE FUNCTION public.text_int_equality (text, int)
  RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 10
RETURN $1::int = $2
;

CREATE OR REPLACE FUNCTION public.text_int_inequality (text, int)
  RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 10
RETURN $1::int != $2
;

create or replace procedure public.create_text_int_equality_operators()
  language plpgsql
as $$
begin
    execute $create$
		CREATE OPERATOR public.= (
		LEFTARG    = text
		, RIGHTARG   = int
		, FUNCTION   = public.text_int_equality
		, COMMUTATOR = OPERATOR(public.=)
		, NEGATOR    = OPERATOR(public.!=)
		)
		;

		CREATE OPERATOR public.!= (
		LEFTARG    = text
		, RIGHTARG   = int
		, FUNCTION   = public.text_int_inequality
		, COMMUTATOR = OPERATOR(public.!=)
		, NEGATOR    = OPERATOR(public.=)
		);
		$create$; 
    exception 
       when sqlstate '42723' then null; 
end;
$$;

CALL create_text_int_equality_operators();

CREATE OR REPLACE FUNCTION public.json_text_equality (json, text)
  RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 10
RETURN $1::text = $2
;

CREATE OR REPLACE FUNCTION public.json_text_inequality (json, text)
  RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 10
RETURN $1::text != $2
;


create or replace procedure public.create_json_text_equality_operators()
  language plpgsql
as $$
begin
    execute $create$
		CREATE OPERATOR public.= (
		LEFTARG    = json
		, RIGHTARG   = text
		, FUNCTION   = public.json_text_equality
		, COMMUTATOR = OPERATOR(public.=)
		, NEGATOR    = OPERATOR(public.!=)
		)
		;

		CREATE OPERATOR public.!= (
		LEFTARG    = json
		, RIGHTARG   = text
		, FUNCTION   = public.json_text_inequality
		, COMMUTATOR = OPERATOR(public.!=)
		, NEGATOR    = OPERATOR(public.=)
		);
		$create$; 
    exception 
       when sqlstate '42723' then null; 
end;
$$;

CALL create_json_text_equality_operators();

CREATE OR REPLACE FUNCTION public.json_extract(col anyelement, path text) RETURNS anyelement language plpgsql IMMUTABLE STRICT as $$
DECLARE
  _arr text[];
BEGIN
  _arr := string_to_array($2, '.');
  _arr := array_remove(_arr, '$');
  return json_extract_path($1::json, VARIADIC _arr);
END;$$
;

create OR REPLACE function public.json_lt(json, json)
returns boolean language sql immutable as $$
    select $1::text < $2::text
$$;
create OR REPLACE function public.json_lte(json, json)
returns boolean language sql immutable as $$
    select $1::text <= $2::text
$$;
create OR REPLACE function public.json_eq(json, json)
returns boolean language sql immutable as $$
    select $1::text = $2::text
$$;
create OR REPLACE function public.json_gte(json, json)
returns boolean language sql immutable as $$
    select $1::text >= $2::text
$$;
create OR REPLACE function public.json_gt(json, json)
returns boolean language sql immutable as $$
    select $1::text > $2::text
$$;
create OR REPLACE function public.json_cmp(json, json)
returns integer language sql immutable as $$
    select case when $1::text < $2::text then -1 when $1::text > $2::text then 1 else 0 end
$$;

create or replace procedure public.create_json_btree_operators()
  language plpgsql
as $$
begin
    execute $create$
		create operator public.< (
			leftarg = json, 
			rightarg = json, 
			procedure = json_lt, 
			commutator = >);
		create operator public.<= (
			leftarg = json, 
			rightarg = json, 
			procedure = json_lte, 
			commutator = >=);
		create operator public.= (
			leftarg = json, 
			rightarg = json, 
			procedure = json_eq, 
			commutator = =);
		create operator public.>= (
			leftarg = json, 
			rightarg = json, 
			procedure = json_gte, 
			commutator = <=);
		create operator public.> (
			leftarg = json, 
			rightarg = json, 
			procedure = json_gt, 
			commutator = <);
		create operator class json_ops
			default for type json using btree as
				operator 1 <,
				operator 2 <=,
				operator 3 =,
				operator 4 >=,
				operator 5 >,
				function 1 json_cmp(json, json);
        $create$; 
    exception 
       when sqlstate '42723' then null; 
end;
$$;

CALL create_json_btree_operators();

CREATE OR REPLACE VIEW sqlite_schema AS
select 'view' AS type, table_name as name, table_name as tbl_name,
	concat('CREATE VIEW \"', table_name, '\" AS ', view_definition) as sql
	from information_schema.views where table_schema = 'public'
UNION
SELECT 'table' as type, table_name as name, table_name as tbl_name, '' as sql
	FROM information_schema.tables WHERE table_schema = 'public'
UNION
select 'index' as type, indexname as name, tablename as tbl_name, indexdef as sql
from pg_indexes WHERE indexname not like '%\_pkey' and schemaname = 'public'
;

CREATE OR REPLACE VIEW sqlite_master as SELECT * FROM sqlite_schema;

create or replace FUNCTION public.PRAGMA_TABLE_INFO(name text)
  returns TABLE (cid numeric, name text, type text, not_null bool, dflt_value text) 
AS
$func$
  SELECT (ordinal_position-1) as cid, column_name as name, upper(data_type) as type, (is_nullable = 'NO') as not_null, replace(column_default, '::text', '') as dflt_value FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1
$func$ 
LANGUAGE sql;