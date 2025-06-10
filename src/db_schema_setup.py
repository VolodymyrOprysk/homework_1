from sqlalchemy import create_engine, text


def create_db_tables(engine):

    with engine.connect() as conn:
        with open('scripts/schema.sql', 'r') as f:
            sql_script = f.read()

        statements = [st.strip()
                      for st in sql_script.split(';') if st.strip()]

        for st in statements:
            conn.execute(text(st))
