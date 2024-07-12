-- INSERT INTO dm.dm_account_balance_f
-- SELECT
-- on_date,
-- account_rk, 
-- balance_out, 
-- balance_out * COALESCE(er.reduced_cource, 1)
-- 	AS balance_out_rub
-- FROM ds.ft_balance_f bal
-- LEFT JOIN ds.md_exchange_rate_d er ON bal.currency_rk = er.currency_rk
-- WHERE (on_date >= er.data_actual_date AND on_date <= er.data_actual_end_date) OR (er.data_actual_date IS NULL);

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Начало заполнения витрины dm.dm_account_balance_f за ' || i_OnDate); 
	
	DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;
	
	INSERT INTO dm.dm_account_balance_f (account_rk, on_date, balance_out, balance_out_rub)
	
	
	SELECT
	    accs.account_rk,
	    i_OnDate AS on_date,
		
	    CASE
	        WHEN accs.char_type = 'А' THEN COALESCE(prev.balance_out, 0) + COALESCE(turn.debet_amount, 0) - COALESCE(turn.credit_amount, 0)
	        ELSE COALESCE(prev.balance_out, 0) + COALESCE(turn.credit_amount, 0) - COALESCE(turn.debet_amount, 0)
	    END AS balance_out,
	    
	    CASE
	        WHEN accs.char_type = 'А' THEN (COALESCE(prev.balance_out, 0) + COALESCE(turn.debet_amount, 0) - COALESCE(turn.credit_amount, 0)) * COALESCE(er.reduced_cource, 1) 
	        ELSE (COALESCE(prev.balance_out, 0) + COALESCE(turn.credit_amount, 0) - COALESCE(turn.debet_amount, 0)) * COALESCE(er.reduced_cource, 1)
	    END AS balance_out_rub
	    
	FROM
		ds.md_account_d accs 
	LEFT JOIN dm.dm_account_turnover_f turn 
		ON accs.account_rk = turn.account_rk AND turn.on_date = i_OnDate
		
	LEFT JOIN dm.dm_account_balance_f prev 
	    ON prev.account_rk = accs.account_rk
	    AND prev.on_date = i_OnDate - interval '1 day'
	
	LEFT JOIN ds.md_exchange_rate_d er 
	    ON accs.currency_rk = er.currency_rk
	    AND (er.data_actual_date <= i_OnDate OR er.data_actual_date IS NULL)
	    AND (er.data_actual_end_date >= i_OnDate OR er.data_actual_end_date IS NULL)
	
	WHERE 
	    (accs.data_actual_date <= i_OnDate OR accs.data_actual_date IS NULL)
	    AND (accs.data_actual_end_date >= i_OnDate OR accs.data_actual_end_date IS NULL);
	
	INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Окончание заполнения витрины dm.dm_account_balance_f за ' || i_OnDate); 
	
	EXCEPTION
    	WHEN OTHERS THEN
        INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Ошибка при заполнении заполнения витрины dm.dm_account_balance_f за ' || i_OnDate || ' : ' || SQLERRM);
END;
$$;

DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN 1..31 LOOP
        CALL ds.fill_account_balance_f((('2018-01-01')::DATE + (d - 1) * interval '1 day')::DATE);
    END LOOP;
END;
$$;

SELECT * FROM dm.dm_account_balance_f ORDER BY account_rk, on_date;