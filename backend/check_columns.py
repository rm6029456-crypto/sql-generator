from simple_query import get_db_connection

def get_columns():
    """Get all column names from the customers table"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'customers'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()
            print("\nColumns in customers table:")
            for col in columns:
                print(f"- {col['column_name']} ({col['data_type']})")
            return columns
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    get_columns()
