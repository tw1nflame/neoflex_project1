CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Начало заполнения витрины dm_account_turnover_f за ' || i_OnDate);
	
	DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;
	 

	INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
	SELECT 
		i_OnDate as on_date,
		COALESCE (credit_data.account_rk, debet_data.account_rk) as account_rk,
		COALESCE(credit_data.credit_amount, 0) AS credit_amount,
        COALESCE(credit_data.credit_amount, 0) * COALESCE(er.reduced_cource, 1) AS credit_amount_rub,
        COALESCE(debet_data.debet_amount, 0) AS debet_amount,
        COALESCE(debet_data.debet_amount, 0) * COALESCE(er.reduced_cource, 1) AS debet_amount_rub
	FROM 
		(SELECT
			credit_account_rk AS account_rk,
			SUM(credit_amount) AS credit_amount
			FROM ds.ft_posting_f WHERE oper_date = i_OnDate
			GROUP BY credit_account_rk
		) as credit_data
	FULL OUTER JOIN 
		(SELECT 
			debet_account_rk AS account_rk,
			SUM(debet_amount) AS debet_amount
			FROM ds.ft_posting_f WHERE oper_date = i_OnDate
			GROUP BY debet_account_rk
		) as debet_data 
	ON credit_data.account_rk = debet_data.account_rk
	LEFT JOIN ds.md_account_d account_info 
		ON COALESCE (credit_data.account_rk, debet_data.account_rk) = account_info.account_rk
	LEFT JOIN ds.md_exchange_rate_d er 
		ON account_info.currency_rk = er.currency_rk
	WHERE 
		(account_info.data_actual_date <= i_OnDate OR account_info.data_actual_date IS NULL) AND
		(account_info.data_actual_end_date >= i_OnDate OR account_info.data_actual_end_date IS NULL) AND
		(er.data_actual_date <= i_OnDate OR er.data_actual_date IS NULL) AND 
		(er.data_actual_end_date >= i_OnDate OR er.data_actual_end_date IS NULL);

	INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Окончание заполнения витрины dm_account_turnover_f за ' || i_OnDate); 

	EXCEPTION WHEN OTHERS THEN
        INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Ошибка при заполнении заполнения витрины dm_account_turnover_f за ' || i_OnDate || ' : ' || SQLERRM);
END;
$$;

DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN 1..31 LOOP
        CALL ds.fill_account_turnover_f((('2018-01-01')::DATE + (d - 1) * interval '1 day')::DATE);
    END LOOP;
END;
$$;

SELECT * FROM dm.dm_account_turnover_f;
