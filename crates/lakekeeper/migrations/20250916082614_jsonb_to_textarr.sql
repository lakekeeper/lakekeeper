CREATE FUNCTION jsonb_to_textarr(jsonb) RETURNS text[] AS $$
    SELECT array_agg(val ORDER BY ord)
    FROM jsonb_array_elements_text($1) WITH ORDINALITY AS e(val, ord);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
