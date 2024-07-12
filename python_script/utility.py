from sqlalchemy import text


def logger(data, engine):
    with engine.connect() as conn:
        query = f"INSERT INTO logs.logs (event_date, event_description) VALUES (NOW(), '{
            data}')"
        conn.execute(text(query))
        conn.commit()
