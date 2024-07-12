CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Начало заполнения витрины dm.dm_f101_round_f за ' || i_OnDate);
	
	DELETE FROM dm.dm_f101_round_f WHERE to_date = i_OnDate - interval '1 day';

	INSERT INTO dm.dm_f101_round_f (from_date, to_date, chapter, ledger_account, characteristic, balance_in_rub, balance_in_val, balance_in_total, 
		turn_deb_rub, turn_deb_val, turn_deb_total, turn_cre_rub, turn_cre_val, turn_cre_total, balance_out_rub, balance_out_val, balance_out_total)
	SELECT
		i_OnDate - interval '1 month' AS from_date,
		i_onDate - interval '1 day' AS to_date,
		ledger.chapter AS chapter,
		SUBSTRING(account_number, 1, 5) AS ledger_account,
		accs.char_type AS characteristic, 

		
		SUM(CASE WHEN accs.currency_code IN ('810', '643') THEN COALESCE(balance_in.balance_out_rub, 0) ELSE 0 END) AS balance_in_rub,
		SUM(CASE WHEN accs.currency_code NOT IN ('810', '643') THEN COALESCE(balance_in.balance_out, 0) ELSE 0 END) AS balance_in_val,
		SUM(COALESCE(balance_in.balance_out_rub, 0)) AS balance_in_total,
		
		SUM(CASE WHEN accs.currency_code IN ('810', '643') THEN COALESCE(turn.debet_amount, 0) ELSE 0 END) AS turn_deb_rub, 
		SUM(CASE WHEN accs.currency_code NOT IN ('810', '643') THEN COALESCE(turn.debet_amount, 0) ELSE 0 END) AS turn_deb_val,
		SUM(COALESCE(turn.debet_amount_rub, 0)) AS turn_deb_total,
		
		SUM(CASE WHEN accs.currency_code IN ('810', '643') THEN COALESCE(turn.credit_amount, 0) ELSE 0 END) AS turn_cre_rub,
		SUM(CASE WHEN accs.currency_code NOT IN ('810', '643') THEN COALESCE(turn.credit_amount, 0) ELSE 0 END) AS turn_cre_val,
		SUM(COALESCE(turn.credit_amount_rub, 0)) AS turn_cre_total,
		
		SUM(CASE WHEN accs.currency_code IN ('810', '643') THEN COALESCE(balance_out.balance_out_rub, 0) ELSE 0 END) AS balance_out_rub,
		SUM(CASE WHEN accs.currency_code NOT IN ('810', '643') THEN COALESCE(balance_out.balance_out_rub, 0) ELSE 0 END) AS balance_out_val,
		SUM(COALESCE(balance_out.balance_out_rub, 0)) AS balance_out_total
		

		FROM ds.md_account_d accs
		
		LEFT JOIN ds.md_ledger_account_s ledger 
			ON SUBSTRING(accs.account_number, 1, 5)::INT = ledger.ledger_account

		LEFT JOIN dm.dm_account_balance_f balance_in 
			ON accs.account_rk = balance_in.account_rk AND balance_in.on_date = i_OnDate - interval '1 month' - interval '1 day'

		LEFT JOIN dm.dm_account_turnover_f turn 
			ON accs.account_rk = turn.account_rk
				AND turn.on_date < i_OnDate and turn.on_date >= i_OnDate - interval '1 month'

		LEFT JOIN dm.dm_account_balance_f balance_out 
			ON accs.account_rk = balance_out.account_rk AND balance_out.on_date = i_OnDate - interval '1 day'

		WHERE accs.data_actual_date <= i_OnDate - interval '1 month' AND accs.data_actual_end_date >= i_OnDate - interval '1 day'
	
		GROUP BY SUBSTRING(accs.account_number, 1, 5), ledger.chapter, accs.char_type;

	INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Окончание заполнения витрины dm.dm_f101_round_f за ' || i_OnDate);

	EXCEPTION WHEN OTHERS THEN
        INSERT INTO logs.procedure_logs (event_date, event_description) VALUES (clock_timestamp(), 'Ошибка при заполнении заполнения витрины dm.dm_f101_round_f за ' || i_OnDate || ' : ' || SQLERRM);

END;
$$;

DELETE FROM dm.dm_f101_round_f;

CALL dm.fill_f101_round_f('2018-02-01');

SELECT * FROM dm.dm_f101_round_f ORDER BY ledger_account;

