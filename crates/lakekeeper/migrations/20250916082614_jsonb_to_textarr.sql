CREATE FUNCTION jsonb_to_textarr(jsonb) RETURNS text[] AS $$
    SELECT array_agg(txt) FROM jsonb_array_elements_text($1) txt;
$$ LANGUAGE SQL;
