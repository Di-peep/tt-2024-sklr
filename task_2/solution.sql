CREATE OR REPLACE PROCEDURE update_dim_user()
LANGUAGE plpgsql
AS $$
BEGIN

	UPDATE dim_user
	SET name = stg.name
	FROM stg_user stg
	WHERE dim_user.user_bk = stg.user_bk AND dim_user.name <> stg.name;

	UPDATE dim_user
	SET _valid_to = NOW()
	FROM stg_user stg
	WHERE dim_user.user_bk = stg.user_bk
		AND (dim_user.country <> stg.country OR dim_user.city <> stg.city)
		AND dim_user._valid_to = '9999-12-31 23:59:59';

	INSERT INTO dim_user (user_bk, name, country, city, _valid_from, _valid_to)
	SELECT 
	    stg.user_bk,
	    stg.name,
	    stg.country,
	    stg.city,
	    NOW() AS _valid_from,
	    '9999-12-31 23:59:59' AS _valid_to
	FROM stg_user stg
	LEFT JOIN dim_user dim
	ON stg.user_bk = dim.user_bk
	WHERE (dim.country <> stg.country OR dim.city <> stg.city OR dim.user_bk IS NULL);

	DELETE FROM stg_user;

END;
$$;


CREATE OR REPLACE FUNCTION trigger_func_update_dim_user()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    CALL update_dim_user();
    RETURN NULL;
END;
$$;


CREATE OR REPLACE TRIGGER trigger_insert_stg_user
AFTER INSERT ON stg_user
FOR EACH STATEMENT
EXECUTE FUNCTION trigger_func_update_dim_user();
