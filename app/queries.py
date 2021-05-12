column_headers = """SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog') and table_name=%s
                    ORDER BY table_schema, table_name"""
