import psycopg2
import re
from psycopg2.extras import RealDictCursor
import os

def get_db_connection():
    """Create a database connection."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        database=os.getenv("DB_NAME", "customers_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        cursor_factory=RealDictCursor
    )

def execute_query(sql: str, params=None):
    """
    Execute a SQL query with optional parameters and return the results.
    
    Args:
        sql (str): The SQL query to execute
        params (dict, optional): Dictionary of parameters for the query
        
    Returns:
        list: List of dictionaries containing the query results
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Print the raw SQL with parameter placeholders
            print(f"[DEBUG] Raw SQL with placeholders:\n{sql}")
            
            # Print the actual SQL that would be executed (for debugging)
            if params:
                actual_sql = sql
                for key, value in params.items():
                    if isinstance(value, str):
                        actual_sql = actual_sql.replace(f'%({key})s', f"'{value}'")
                    else:
                        actual_sql = actual_sql.replace(f'%({key})s', str(value))
                print(f"[DEBUG] Actual SQL with values substituted:\n{actual_sql}")
                
                print(f"[DEBUG] Executing with params: {params}")
                cur.execute(sql, params)
            else:
                print("[DEBUG] Executing without parameters")
                cur.execute(sql)
                
            if sql.strip().lower().startswith('select'):
                results = cur.fetchall()
                return [dict(row) for row in results]
            conn.commit()
            return []
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def query_database(query: str):
    """Execute a query and return the results."""
    conn = None
    try:
        # First, parse the query to get the SQL
        parsed = parse_simple_query(query)
        
        # If parse_simple_query returned a complete response (like for list all customers), return it
        if isinstance(parsed, dict) and 'type' in parsed and 'rows' in parsed:
            return parsed
            
        # If parse_simple_query returned a dictionary (like for count queries), handle it
        if isinstance(parsed, dict):
            # If it's already a complete response with a value, return it
            if 'type' in parsed and 'value' in parsed and parsed['value'] is not None:
                return parsed
                
            # If it's a metric with SQL to execute
            if parsed.get('type') == 'metric' and 'sql' in parsed:
                # Get the SQL query
                sql = parsed['sql']
                
                # If sql is a dictionary, try to extract the actual SQL
                if isinstance(sql, dict):
                    sql = sql.get('sql', '')
                
                # If we don't have a valid SQL string, return an error
                if not isinstance(sql, str) or not sql.strip():
                    return {
                        "type": "error",
                        "message": "No valid SQL query was generated",
                        "query": query,
                        "sql": str(sql) if sql else ""
                    }
                
                try:
                    # Execute the SQL query directly
                    conn = get_db_connection()
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(sql)
                        result = cur.fetchone()
                        
                        # Get the count value from the result
                        count_value = 0
                        if result:
                            # Get the first column value (for count queries)
                            count_value = list(result.values())[0] if result else 0
                            
                        # Update the parsed result with the count
                        parsed['value'] = int(count_value) if count_value is not None else 0
                        
                        # Ensure we're returning the full response with the value
                        return {
                            'type': 'metric',
                            'label': parsed.get('label', 'Total Customers'),
                            'value': parsed['value'],
                            'query': query,
                            'sql': parsed.get('sql', '')
                        }
                        
                except Exception as e:
                    return {
                        "type": "error",
                        "message": f"Error executing query: {str(e)}",
                        "query": query,
                        "sql": sql
                    }
                    
                finally:
                    if sql.strip().lower().startswith('select'):
                        results = cur.fetchall()
                        rows_data = [dict(row) for row in results]
                        return {
                            'type': 'table',
                            'columns': list(rows_data[0].keys()) if rows_data else [],
                            'rows': rows_data,
                            'query': natural_query,
                            'sql': sql
                        }
                    else:
                        conn.commit()
                        return {
                            'type': 'success',
                            'message': f'Query executed successfully. Rows affected: {cur.rowcount}',
                            'query': natural_query,
                            'sql': sql
                        }
                
    except Exception as e:
        print(f"[ERROR] Query failed: {str(e)}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return {
            'type': 'error',
            'message': str(e),
            'query': natural_query,
            'sql': query_result.get('sql', str(query_result)) if isinstance(query_result, dict) else str(query_result)
        }
    finally:
        if 'conn' in locals():
            conn.close()

def parse_where_condition(condition: str) -> str:
    """Parse a natural language condition into SQL WHERE clause."""
    # Handle simple conditions like "age > 30"
    condition = condition.strip()
    
    # Handle AND/OR conditions
    if ' and ' in condition.lower():
        parts = [f"({parse_where_condition(part.strip())})" 
                for part in re.split(r'\s+and\s+', condition, flags=re.IGNORECASE)]
        return ' AND '.join(parts)
    elif ' or ' in condition.lower():
        parts = [f"({parse_where_condition(part.strip())})" 
                for part in re.split(r'\s+or\s+', condition, flags=re.IGNORECASE)]
        return ' OR '.join(parts)
    
    # Define column types and their aliases
    column_aliases = {
        'age': ['age', 'years old', 'years'],
        'spending_score': ['spending_score', 'spending score', 'spend score', 'spending'],
        'annual_income_k': ['annual_income_k', 'income', 'annual income', 'salary', 'earnings'],
        'credit_score': ['credit_score', 'credit', 'credit score', 'credit rating'],
        'loyalty_years': ['loyalty_years', 'loyalty', 'loyalty years', 'years of loyalty', 'customer since'],
        'customerid': ['customerid', 'id', 'customer id', 'client id'],
        'gender': ['gender', 'sex'],
        'preferred_category': ['preferred_category', 'category', 'preferred category', 'shopping category'],
        'age_group': ['age_group', 'age group', 'generation'],
        'estimated_savings_k': ['estimated_savings_k', 'savings', 'estimated savings', 'savings amount']
    }
    
    # Create reverse mapping from alias to column name
    alias_to_column = {}
    for col, aliases in column_aliases.items():
        for alias in aliases:
            alias_to_column[alias] = col
    
    # Define numeric columns for specific operator handling
    numeric_columns = ['age', 'spending_score', 'annual_income_k', 'credit_score', 'loyalty_years', 'customerid', 'estimated_savings_k']
    text_columns = ['gender', 'preferred_category', 'age_group']
    
    # Handle comparison operators with natural language support
    operator_map = {
        '>=': ['greater than or equal to', 'at least', 'minimum', 'minimum of', 'or more', 'and above', 'and higher'],
        '<=': ['less than or equal to', 'at most', 'maximum', 'maximum of', 'or less', 'and below', 'and lower'],
        '!=': ['not equal to', 'not equal', 'is not', 'does not equal', 'different from'],
        '>': ['greater than', 'more than', 'over', 'older than', 'above', 'higher than', 'exceeds'],
        '<': ['less than', 'fewer than', 'under', 'younger than', 'below', 'lower than'],
        '=': ['equals', 'is', ':', 'are', 'exactly', 'equal to', 'same as'],
        'like': ['contains', 'like', 'matching', 'includes', 'with', 'that contains', 'having'],
        'not like': ['does not contain', 'not containing', 'without', 'excluding']
    }
    
    # Find the operator in the condition
    condition_lower = f' {condition.lower()} '
    
    # First, handle special cases for text columns with 'contains' or 'with'
    for field in text_columns:
        for term in ['containing', 'with', 'that has', 'having']:
            if f' {term} {field} ' in condition_lower:
                parts = re.split(f'\s+{re.escape(term)}\s+{re.escape(field)}\s+', condition, flags=re.IGNORECASE)
                if len(parts) == 2:
                    value = parts[1].strip(" '")
                    return f"LOWER({field}) LIKE LOWER('%{value}%')"
    
    # Handle standard operators
    for op, aliases in operator_map.items():
        for alias in aliases:
            # Special handling for 'with' as it's common in natural language
            if alias == 'with' and 'with' in condition_lower and not any(f' {f} ' in condition_lower for f in text_columns):
                continue
                
            if f' {alias} ' in condition_lower:
                parts = re.split(f'\s+{re.escape(alias)}\s+', condition, flags=re.IGNORECASE)
                if len(parts) == 2:
                    field_part = parts[0].strip()
                    value_part = parts[1].strip(" '")
                    
                    # Try to find the column name in the field part
                    field = None
                    for col, aliases in column_aliases.items():
                        for a in aliases:
                            if field_part.lower().endswith(f' {a}'):
                                field = col
                                break
                        if field:
                            break
                    
                    if not field:
                        # If no known column found, use the last word as field name
                        field = field_part.split()[-1] if field_part.split() else field_part
                    
                    # Clean up the value
                    value = value_part.split(' and ')[0].strip()  # Handle 'and' in values
                    
                    # Handle different column types
                    if field in text_columns and op not in ['>', '<', '>=', '<=']:
                        if op in ['like', 'not like']:
                            return f"LOWER({field}) {op.upper()} LOWER('%{value}%')"
                        return f"LOWER({field}) = LOWER('{value}')"
                    elif field.replace('_', '') in numeric_columns and value.replace('.', '').isdigit():
                        return f"{field} {op} {value}"
                    # Fallback for unknown column types
                    return f"{field} {op} '{value}'"
    
    # Handle direct comparisons like "annual_income_k > 19"
    comparison_ops = ['>=', '<=', '!=', '>', '<', '=']
    for op in comparison_ops:
        if op in condition:
            parts = condition.split(op, 1)
            if len(parts) == 2:
                field = parts[0].strip()
                value = parts[1].strip()
                # Check if the field is a valid column name
                if field in [col for col_list in column_aliases.values() for col in col_list] + list(column_aliases.keys()):
                    # If it's a numeric column, don't quote the value
                    if any(field in aliases for field_name, aliases in column_aliases.items() if field_name in numeric_columns):
                        return f"{field} {op} {value}"
                    else:
                        return f"{field} {op} '{value}'"
    
    # Default to equality if no operator found
    if ' ' in condition:
        field, value = condition.split(maxsplit=1)
        value = value.strip(" '")
        # Check if it's a numeric column
        if any(field in aliases for field_name, aliases in column_aliases.items() if field_name in numeric_columns):
            return f"{field} = {value}"
        else:
            return f"{field} = '{value}'"
    
    return condition

# Common stop words to be removed from queries
STOP_WORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'to', 'of', 'in', 'on', 'at', 'by', 'for', 'with', 'about', 'as', 'into', 'like', 'through',
    'after', 'over', 'between', 'out', 'against', 'during', 'before', 'above', 'below', 'from',
    'up', 'down', 'in', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once',
    'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
    'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than',
    'too', 'very', 'can', 'will', 'just', 'should', 'now', 'that', 'this', 'these', 'those',
    'show', 'list', 'get', 'give', 'find', 'me', 'my', 'mine', 'our', 'ours', 'you', 'your',
    'yours', 'their', 'theirs', 'which', 'who', 'whom', 'whose', 'what', 'which', 'whose',
    'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'would', 'could',
    'should', 'may', 'might', 'must', 'shall', 'will', 'can', 'could', 'would', 'should',
    'might', 'must', 'ought', "i'm", "you're", "he's", "she's", "it's", "we're", "they're",
    "i've", "you've", "we've", "they've", "i'll", "you'll", "he'll", "she'll", "it'll",
    "we'll", "they'll", "i'd", "you'd", "he'd", "she'd", "it'd", "we'd", "they'd",
    "isn't", "aren't", "wasn't", "weren't", "hasn't", "haven't", "hadn't", "don't",
    "doesn't", "didn't", "won't", "wouldn't", "can't", "cannot", "couldn't", "mustn't"
}

def remove_stop_words(text):
    """Remove common stop words from the text."""
    if not text or not isinstance(text, str):
        return text
        
    words = text.split()
    filtered_words = [word for word in words if word.lower().strip("'\".,;:!?") not in STOP_WORDS]
    return ' '.join(filtered_words)

def is_valid_query(query):
    """Check if the query contains any valid patterns."""
    query_lower = query.lower()
    
    # List of valid patterns or keywords that indicate a valid query
    valid_patterns = [
        # Customer related
        'customer', 'clients?', 'users?', 'people', 'persons?',
        # Actions
        'show', 'list', 'find', 'get', 'select', 'count', 'total', 'number of', 'how many',
        # Common fields
        'name', 'age', 'gender', 'income', 'score', 'location', 'city', 'country', 'state',
        # Common conditions
        'where', 'with', 'having', 'group by', 'order by', 'sort by', 'limit',
        # Operators
        '>', '<', '=', '!=', '>=', '<=', 'between', 'like', 'in', 'not in',
        # Common values
        'male', 'female', 'high', 'low', 'medium', 'top', 'bottom', 'average', 'sum', 'min', 'max'
    ]
    
    # Check if any valid pattern is in the query
    return any(re.search(r'\b' + re.escape(p) + r'\b', query_lower) for p in valid_patterns)

def parse_simple_query(query):
    """
    Convert a natural language query to SQL.
    
    Supported patterns:
    - Basic queries: 'show me all customers', 'list female customers'
    - Filtering: 'customers with age > 30', 'high income customers', 'customers under 19'
    - Aggregations: 'average income by gender', 'total sales by month'
    - Grouping: 'group by country', 'segment by age group'
    - Sorting: 'top 10 highest spenders', 'lowest performing products'
    - Time-based: 'sales this month', 'new customers last 30 days'
    - Complex queries: 'customers who purchased last month but not this month'
    - Generic WHERE: 'customers where age > 30 and gender = "Female"'
    - Count queries: 'count customers', 'how many users', 'total number of orders'
    
    Returns:
        dict: A dictionary containing the query result or an error message
    """
    # Check for empty or invalid query
    if not query or not query.strip() or not any(c.isalnum() for c in query):
        return {
            'type': 'error',
            'message': 'Please enter a valid query',
            'suggestions': [
                'Show me all customers',
                'Count female customers',
                'List customers with age > 30',
                'Show average income by gender'
            ]
        }
    
    # Check if query contains any valid patterns
    if not is_valid_query(query):
        return {
            'type': 'error',
            'message': 'Sorry, I didn\'t understand your query. Here are some examples:',
            'suggestions': [
                'Show me all customers',
                'Count female customers',
                'List customers with age > 30',
                'Show average income by gender'
            ]
        }
    
    print(f"[DEBUG] Original query: {query}")
    original_query = query
    params = {}  # Initialize params dictionary to store query parameters
    
    # Initialize query components
    select = ['*']
    from_table = 'customers'
    where_conditions = []
    order_by = ['customerid']
    limit = 1000
    params = {}
    
    # Check for specific conditions in the query
    if 'where' in query.lower():
        # Extract the condition part after 'where'
        where_parts = re.split(r'where\s+', query, flags=re.IGNORECASE)
        if len(where_parts) > 1:
            condition = where_parts[1].split(' order by ')[0].split(' limit ')[0]
            where_conditions.append(parse_where_condition(condition))
    
    # Check for direct column conditions (e.g., "annual_income_k > 19")
    for col in ['annual_income_k', 'spending_score', 'age', 'credit_score', 'loyalty_years', 'estimated_savings_k']:
        # Look for patterns like "column > 19" or "column greater than 19"
        patterns = [
            (f'{col}\s*(>|>=|<|<=|=|!=)\s*(\d+)', 1, 2),  # column > 19
            (f'{col}\s+(greater than|less than|more than|over|under|at least|at most|exactly)\s+(\d+)', 1, 2),  # column greater than 19
            (f'(>|>=|<|<=|=|!=)\s*(\d+)\s+{col}', 1, 2),  # > 19 column
            (f'(greater than|less than|more than|over|under|at least|at most|exactly)\s+(\d+)\s+{col}', 1, 2)  # greater than 19 column
        ]
        
        for pattern, op_group, val_group in patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                op_map = {
                    '>': '>', '<': '<', '>=': '>=', '<=': '<=', '=': '=', '!=': '!=',
                    'greater than': '>', 'more than': '>', 'over': '>', 'above': '>',
                    'less than': '<', 'under': '<', 'below': '<',
                    'at least': '>=', 'at most': '<=', 'exactly': '='
                }
                op = op_map.get(match.group(op_group).lower(), '=')
                value = match.group(val_group)
                where_conditions.append(f"{col} {op} {value}")
                break
    conditions = []  # Initialize conditions list
    
    # Handle count queries first
    count_match = re.search(r'(?i)(count|number of|how many|total(?: number of)?)\s+(?:the\s+)?(\w+)(?:\s+customers?)?(?:\s+who are)?(?:\s+that are)?(?:\s+that is)?(?:\s+that was)?\s*(.*)', query)
    if count_match:
        # Extract the entity and any additional conditions
        entity = count_match.group(2).lower()
        additional_conditions = count_match.group(3).strip() if count_match.group(3) else ''
        
        # Handle gender-specific counts
        if 'female' in query.lower() or 'woman' in query.lower() or 'women' in query.lower():
            sql = "SELECT COUNT(*) AS count FROM customers WHERE LOWER(gender) = 'female'"
            label = 'Total Female Customers'
        elif 'male' in query.lower() or 'man' in query.lower() or 'men' in query.lower():
            sql = "SELECT COUNT(*) AS count FROM customers WHERE LOWER(gender) = 'male'"
            label = 'Total Male Customers'
        # Handle general customer counts
        elif entity in ['customer', 'customers']:
            sql = "SELECT COUNT(*) AS count FROM customers"
            label = 'Total Customers'
        else:
            # For other entity types, just count from the specified table
            sql = f"SELECT COUNT(*) AS count FROM {entity}"
            label = f'Total {entity.capitalize()}'
            
        # Add any additional conditions
        if additional_conditions:
            if 'where' not in additional_conditions.lower():
                sql += ' WHERE ' + additional_conditions
            else:
                sql += ' ' + additional_conditions
                
        print(f"[DEBUG] Generated count SQL: {sql}")
        return {
            'type': 'metric',
            'sql': sql,
            'params': params,
            'label': label,
            'value': None  # Will be filled in by query_database
        }
        
    # Build the SQL query for non-count queries
    sql_parts = ["SELECT", ", ".join(select), "FROM", from_table]
    
    if where_conditions:
        sql_parts.extend(["WHERE", " AND ".join(where_conditions)])
    
    if order_by:
        sql_parts.extend(["ORDER BY", ", ".join(order_by)])
    
    if limit:
        sql_parts.extend(["LIMIT", str(limit)])
    
    sql = " ".join(sql_parts)
    
    # Prepare the response
    return {
        'type': 'table',
        'sql': sql,
        'params': params,
        'label': f'Customers {query}',
        'data': None  # Will be filled in by query_database
    }
    
    # Convert to lowercase and remove stop words from the query for other processing
    query = query.lower().strip()
    query = remove_stop_words(query)
    
    # Initialize query components
    select = []
    from_table = "customers"
    where_conditions = []
    group_by = []
    having_conditions = []
    order_by = []
    limit = None
    is_distinct = False
    
    # Track which conditions we've already processed
    processed_conditions = set()
    
    # Helper function to add conditions
    def add_condition(*args, **kwargs):
        is_text = kwargs.get('is_text', False)
        
        if len(args) == 1:
            # Single argument case: just add the condition as is
            condition = args[0]
            if is_text and not isinstance(condition, (int, float)) and not (isinstance(condition, str) and (condition.startswith("'") and condition.endswith("'")) or condition.replace('.', '').isdigit()):
                condition = f"'{condition}'"
            where_conditions.append(condition)
        elif len(args) == 3:
            # Three arguments case: field, operator, value
            field, op, value = args
            if is_text and not isinstance(value, (int, float)) and not (isinstance(value, str) and (value.startswith("'") and value.endswith("'")) or value.replace('.', '').isdigit()):
                value = f"'{value}'"
            where_conditions.append(f"{field} {op} {value}")
        else:
            raise ValueError("add_condition() takes 1 or 3 positional arguments")
        
    def add_simple_condition(field, operator, value, is_text=False):
        if is_text and not (value.startswith("'") and value.endswith("'")) and not value.replace('.', '').isdigit():
            value = f"'{value}'"
        where_conditions.append(f"{field} {operator} {value}")
        
    def parse_where_clause(where_str):
        """Parse a generic WHERE clause from natural language"""
        try:
            # Handle simple column comparisons
            if '=' in where_str or '>' in where_str or '<' in where_str or '!=' in where_str:
                # Try to split into column, operator, value
                parts = re.split(r'(>|<|>=|<=|=|!=|<>|like|ilike|not like|not ilike)', where_str, 1)
                if len(parts) >= 3:
                    col = parts[0].strip()
                    op = parts[1].strip()
                    val = parts[2].strip()
                    
                    # Handle string values
                    if val.startswith("'") and val.endswith("'"):
                        val = val.strip("'")
                    
                    # Handle numeric values
                    if val.replace('.', '').isdigit():
                        return f"{col} {op} {val}", []
                    else:
                        return f"{col} {op} '{val}'", []
            
            # Handle AND/OR conditions
            if ' and ' in where_str.lower():
                parts = re.split(r'\s+and\s+', where_str, 1, re.IGNORECASE)
                left, lparams = parse_where_clause(parts[0])
                right, rparams = parse_where_clause(parts[1])
                return f"({left}) AND ({right})", lparams + rparams
                
            if ' or ' in where_str.lower():
                parts = re.split(r'\s+or\s+', where_str, 1, re.IGNORECASE)
                left, lparams = parse_where_clause(parts[0])
                right, rparams = parse_where_clause(parts[1])
                return f"({left}) OR ({right})", lparams + rparams
            
            # Default case - return as is
            return where_str, []
            
        except Exception as e:
            print(f"[WARNING] Error parsing WHERE clause: {e}")
            return "1=1", []  # Default to true condition to avoid syntax errors
            
        # Handle individual conditions
        patterns = [
            # Numeric comparisons: age > 30, income >= 50000
            (r'([a-z_]+)\s*(>|<|>=|<=|=|!=|<>|like|ilike|not like|not ilike|in|not in|between|is|is not)\s*([^\s\'\"]+|\'.*?\'|\d+(?:\.\d+)?)', re.IGNORECASE),
            # String comparisons: name = 'John', email like '%@gmail.com'
            (r'([a-z_]+)\s*(=|<|>|>=|<=|!=|<>|like|ilike|not like|not ilike|in|not in|between|is|is not)\s*([^\s\'\"]+|\'.*?\'|\d+(?:\.\d+)?)', re.IGNORECASE),
            # IN conditions: status in ('active', 'pending')
            (r'([a-z_]+)\s+in\s*(\([^)]+\))', re.IGNORECASE),
            # BETWEEN conditions: age between 20 and 30
            (r'([a-z_]+)\s+between\s+(\d+)\s+and\s+(\d+)', re.IGNORECASE),
            # IS NULL/IS NOT NULL: email is null
            (r'([a-z_]+)\s+is\s+(not\s+)?(null|true|false)', re.IGNORECASE)
        ]
        
        for pattern, flags in patterns:
            match = re.match(pattern, where_str, flags)
            if match:
                groups = match.groups()
                field = groups[0]
                
                # Handle different condition types
                if len(groups) == 2:  # Simple comparison
                    op, value = groups
                    # Clean up the value
                    if value.startswith("'") and value.endswith("'"):
                        value = value.strip("'")
                    return f"{field} {op} %s", [value]
                elif len(groups) == 3 and 'between' in where_str:  # BETWEEN
                    _, start, end = groups
                    return f"{field} BETWEEN %s AND %s", [start, end]
                elif 'is' in where_str.lower():  # IS NULL/IS NOT NULL
                    not_null = 'NOT ' if groups[1] else ''
                    return f"{field} IS {not_null}{groups[2].upper()}", []
                elif 'in' in where_str.lower():  # IN clause
                    values = [v.strip(" '\"") for v in groups[1].strip('()').split(',')]
                    placeholders = ', '.join(['%s'] * len(values))
                    return f"{field} IN ({placeholders})", values
        
        # If no pattern matched, return the original condition as a LIKE search
        return f"{where_str} LIKE %s", [f"%{where_str}%"]
    
    # Define numeric and text columns
    numeric_columns = ['age', 'spending_score', 'annual_income_k', 'credit_score', 'loyalty_years', 'customerid']
    text_columns = ['gender', 'preferred_category', 'age_group']
    all_columns = numeric_columns + text_columns
    column_pattern = '|'.join(re.escape(col) for col in all_columns)
    
    # Handle list all customers queries
    list_customers_match = re.search(
        r'(?:show|list)(?:\s+all)?(?:\s+the)?\s+customers$',
        query,
        re.IGNORECASE
    )
    
    if list_customers_match:
        return "SELECT * FROM customers"
        
    # Map natural language column names to database column names
    column_mapping = {
        'age': 'age',
        'income': 'annual_income_k',
        'annual_income': 'annual_income_k',
        'annual_income_k': 'annual_income_k',  # Add direct mapping for annual_income_k
        'spending': 'spending_score',
        'spending_score': 'spending_score',
        'credit': 'credit_score',
        'credit_score': 'credit_score',
        'loyalty': 'loyalty_years',
        'loyalty_years': 'loyalty_years',
        'savings': 'estimated_savings_k',
        'estimated_savings': 'estimated_savings_k',
        'gender': 'gender',
        'id': 'customerid',
        'customerid': 'customerid',
        'age_group': 'age_group',
        'preferred_category': 'preferred_category'
    }
    
    # First check for gender condition
    gender_match = re.search(
        r'(?:show|list)(?:\s+all)?(?:\s+the|\s+of|\s+customers)?(?:\s+with)?(?:\s+where)?\s+'  # Prefix
        r'(?:customers\s+)?'  # Optional 'customers' keyword
        r'(male|female|non-binary|nonbinary|other)',  # Gender
        query,
        re.IGNORECASE
    )
    
    # Handle 'whose' clause for conditions like 'female customers whose annual_income_k less than 20' or 'customers whose age_group is null/not null'
    whose_condition_match = re.search(
        r'(?:show|list)(?:\s+all)?(?:\s+the|\s+of|\s+customers)?\s+'  # Prefix
        r'(?:(male|female|non-binary|nonbinary|other)\s+)?'  # Optional gender
        r'customers\s+whose\s+'  # 'customers whose' clause
        r'([a-zA-Z_][\w_]*(?:\s*_\s*[\w_]+)*)\s*'  # Column name
        r'(?:\s*(?:is\s+)?(not\s+null|not\s+none|notnull|notnone|not\s+set|is\s+set|is\s+not\s+null|is\s+not\s+none|is\s+notnull|is\s+notnone|is\s+set|is\s+not\s+set|'  # Special NULL/IS NULL/IS NOT NULL cases
        r'<|>|<=|>=|=|!=|less than|greater than|at least|at most|more than|below|above|under|over|is|equals?|equal to|is equal to)\s*'  # Standard operators
        r'((?:\d+(?:\.\d+)?)|null|not null|NULL|NOT NULL|None|NONE|notnone|NOTNONE)?)?'  # Value (optional for IS NULL)
        r'(?:\s+and|\s*$|\s*;|\s*,|\s+with|\s+where|\s+and\s+the|\s+and\s+for|\s+and\s+with|\s*$)',  # End of condition
        query,
        re.IGNORECASE
    )
    
    # Check for any column with comparison operators
    with open('debug.log', 'a') as f:
        f.write(f"[DEBUG] Processing query: {query}\n")
    
    # This pattern will match any word character sequence as a column name
    condition_match = re.search(
        r'(?:show|list)(?:\s+all)?(?:\s+the|\s+of|\s+customers)?(?:\s+with)?(?:\s+where)?\s+'  # Prefix
        r'(?:customers\s+)?'  # Optional 'customers' keyword
        r'(?:whose\s+)?'  # Optional 'whose' keyword
        r'([a-zA-Z_][\w_]*(?:\s*_\s*[\w_]+)*)\s+'  # Column name (must start with letter or underscore)
        r'(<|>|<=|>=|=|!=|less than|greater than|at least|at most|more than|below|above|under|over|is|equals?|equal to|is equal to)\s*'  # Operator
        r'(\d+(?:\.\d+)?)(?:\s+and|\s*$|\s*;|\s*,|\s+with|\s+where|\s+and\s+the|\s+and\s+for|\s+and\s+with|\s*$)',  # Value (number with optional decimal)
        query,
        re.IGNORECASE
    )
    
    with open('debug.log', 'a') as f:
        if condition_match:
            f.write(f"[DEBUG] Condition matched - Column: {condition_match.group(1)}, Operator: {condition_match.group(2)}, Value: {condition_match.group(3)}\n")
        else:
            f.write("[DEBUG] No condition matched\n")
    
    # Build the WHERE clause
    conditions = []
    
    # Add gender condition if present
    if gender_match:
        gender = gender_match.group(1).lower()
        # Map variations to standard values
        gender_map = {
            'male': 'Male',
            'female': 'Female',
            'non-binary': 'Non-binary',
            'nonbinary': 'Non-binary',
            'other': 'Other'
        }
        gender_value = gender_map.get(gender.lower(), gender)
        conditions.append(f"gender = '{gender_value}'")
    
    # Add condition if present from the standard pattern
    if condition_match:
        col = condition_match.group(1).strip().lower().replace(' ', '_')  # Normalize column name
        op = condition_match.group(2).lower().strip()
        val = condition_match.group(3).strip("'\"")
        with open('debug.log', 'a') as f:
            f.write(f"[DEBUG] Processing condition - Column: {col}, Operator: {op}, Value: {val}\n")
            f.write(f"[DEBUG] Column mapping: {column_mapping.get(col, 'Not found')}\n")
        
        # Map column name to database column name using the main column_mapping
        col = column_mapping.get(col, col)
        
        # Special handling for annual_income_k
        if col == 'annual_income' or col == 'income':
            col = 'annual_income_k'
        
        # Handle different operator formats
        if op in ['less than', 'below', 'under']:
            op = '<'
        elif op in ['greater than', 'above', 'over', 'more than']:
            op = '>'
        elif op == 'at least':
            op = '>='
        elif op == 'at most':
            op = '<='
        elif op in ['is', 'equal', 'equals', 'equal to', 'is equal to']:
            op = '='
        
        # Clean up the value
        val = val.strip("'\" ")
        
        # Special handling for age conditions
        if col == 'age':
            try:
                age = int(val)
                # Use parameterized query to avoid SQL injection and type issues
                param_name = f"age_{op}_{age}"
                conditions.append(f"{col} {op} %({param_name})s")
                params[param_name] = age
            except ValueError:
                # If we can't convert to int, try to handle as string
                conditions.append(f"{col}::text {op} %s")
                params[f"{col}_str"] = val
        # Handle different value types for other columns
        elif col in ['gender', 'age_group', 'preferred_category']:
            # For text columns, use string comparison with quotes
            conditions.append(f"LOWER({col}) = LOWER('{val}')")
        else:
            # For numeric columns, use direct comparison
            try:
                # Try to convert to float first, then int if no decimal
                num_val = float(val)
                if num_val.is_integer():
                    num_val = int(num_val)
                conditions.append(f"{col} {op} {num_val}")
            except ValueError:
                # If conversion fails, treat as string
                conditions.append(f"{col} {op} '{val}'")
    
    # Handle 'whose' condition if present
    whose_condition_match = re.search(
        r'(?:whose|with|where|that have|that has|that are|who have|who has|who are)\s+'
        r'(?:(male|female|non[- ]?binary|other)\s+)?'
        r'(?:customers?\s+)?(?:that\s+)?(?:have\s+)?'
        r'(\w+(?:\s+\w+)*?)\s+'
        r'(?:is\s+)?(less than|greater than|more than|at least|at most|over|under|below|above|equal to|equals?|is(?:\s+not)?|not)?\s*'
        r'([^\s].*?)(?:\s+and|\s+or|\s*$|\s*;)',
        query,
        re.IGNORECASE
    )
    
    if whose_condition_match:
        gender = (whose_condition_match.group(1) or '').strip()
        col = (whose_condition_match.group(2) or '').strip().lower().replace(' ', '_')  # Normalize column name
        op = (whose_condition_match.group(3) or '=').lower().strip()
        val = (whose_condition_match.group(4) or '').strip("'\" ")
        
        # Debug logging
        print(f"[DEBUG] 'whose' condition - Column: {col}, Operator: '{op}', Value: '{val}'")
        
        with open('debug.log', 'a') as f:
            f.write(f"[DEBUG] Processing 'whose' condition - Gender: {gender}, Column: {col}, Operator: {op}, Value: {val}\n")
        
        # Add gender condition if specified
        if gender:
            gender_map = {
                'male': 'Male',
                'female': 'Female',
                'non-binary': 'Non-binary',
                'nonbinary': 'Non-binary',
                'other': 'Other'
            }
            gender_value = gender_map.get(gender.lower(), gender)
            conditions.append(f"gender = '{gender_value}'")
        
        # Map column name to database column name
        col = column_mapping.get(col, col)
        
        # Special handling for annual_income_k
        if col == 'annual_income' or col == 'income':
            col = 'annual_income_k'
        
        # Handle NULL/IS NULL/IS NOT NULL conditions
        if not op or any(x in (op or '').lower() + ' ' + (val or '').lower() for x in ['null', 'none', 'notnull', 'notnone']):
            # Check for NOT NULL patterns
            if any(x in (op or '').lower() + ' ' + (val or '').lower() for x in ['not null', 'notnull', 'not none', 'notnone', 'is not null', 'is not none']):
                conditions.append(f"{col} IS NOT NULL")
            # Check for NULL patterns
            elif any(x in (op or '').lower() + ' ' + (val or '').lower() for x in ['null', 'none']):
                conditions.append(f"{col} IS NULL")
            # Default to IS NULL if just the column is specified (e.g., 'whose age_group')
            elif not op and not val:
                conditions.append(f"{col} IS NOT NULL")
        else:
            # Handle different operator formats
            op = op.lower().strip()
            if any(x in op for x in ['less than', 'below', 'under']):
                op = '<'
            elif any(x in op for x in ['greater than', 'above', 'over', 'more than']):
                op = '>'
            elif 'at least' in op:
                op = '>='
            elif 'at most' in op or 'less than or equal' in op:
                op = '<='
            elif any(x in op for x in ['equal to', 'equals', 'equal', 'is', '=']):
                op = '='
            elif 'not equal' in op or '!=' in op:
                op = '!='
            
            # Clean up the value
            val = val.strip("'\" ")
            
            # Special handling for age conditions
            if col == 'age':
                try:
                    age = int(val)
                    param_name = f"age_{op}_{age}"
                    conditions.append(f"age {op} %({param_name})s")
                    params[param_name] = age
                except ValueError:
                    # If we can't convert to int, try to handle as string
                    param_name = f"age_{op}_str"
                    conditions.append(f"age::text {op} %({param_name})s")
                    params[param_name] = val
            # Handle different value types for other columns
            elif col in ['gender', 'age_group', 'preferred_category']:
                # For text columns, use string comparison with quotes
                conditions.append(f"LOWER({col}) = LOWER('{val}')")
            else:
                # For numeric columns, use direct comparison
                try:
                    # Try to convert to float first, then int if no decimal
                    num_val = float(val)
                    if num_val.is_integer():
                        num_val = int(num_val)
                    conditions.append(f"{col} {op} {num_val}")
                except ValueError:
                    # If conversion fails, treat as string
                    conditions.append(f"{col} {op} '{val}'")
    
    # If we have any conditions, build the query
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
    else:
        where_clause = ""
    
    # Build the final query with proper parameterization
    base_query = "SELECT * FROM customers"
    
    # Debug logging
    final_sql = base_query + where_clause
    print(f"[DEBUG] Final SQL: {final_sql}")
    if params:
        print(f"[DEBUG] Parameters: {params}")
    
    if params:
        # If we have parameters, return a dictionary with the query and params
        return {
            'sql': final_sql,
            'params': params,
            'type': 'query'
        }
    else:
        # If no parameters, return the query as a string
        return final_sql
    
    # If no specific conditions, check if it's just a basic list customers query
    basic_list_match = re.search(
        r'^(?:show|list)(?:\s+all)?(?:\s+the|\s+of|\s+customers)?$',
        query.strip(),
        re.IGNORECASE
    )
    if basic_list_match:
        return "SELECT * FROM customers"
    
    # Handle between queries
    between_match = re.search(
        r'(?:show|list)(?:\s+all)?(?:\s+the|\s+of|\s+customers)?(?:\s+with)?(?:\s+where)?\s+'  # Prefix
        r'(?:customers\s+)?'  # Optional 'customers' keyword
        r'(age|income|spending|credit|loyalty|savings)\s+'  # Column
        r'between\s+'  # Operator
        r'(\d+)\s+and\s+(\d+)',  # Values
        query,
        re.IGNORECASE
    )
    
    if between_match:
        # Check for gender condition in the same query
        gender_match = re.search(
            r'(?:show|list)(?:\s+all)?(?:\s+the|\s+of|\s+customers)?(?:\s+with)?(?:\s+where)?\s+'  # Prefix
            r'(?:customers\s+)?'  # Optional 'customers' keyword
            r'(male|female|non-binary|nonbinary|other)',  # Gender
            query,
            re.IGNORECASE
        )
        
        # Build conditions
        conditions = []
        
        # Add gender condition if present
        if gender_match:
            gender = gender_match.group(1).lower()
            gender_map = {
                'male': 'Male',
                'female': 'Female',
                'non-binary': 'Non-binary',
                'nonbinary': 'Non-binary',
                'other': 'Other'
            }
            gender_value = gender_map.get(gender.lower(), gender)
            conditions.append(f"gender = '{gender_value}'")
        
        # Add between condition
        col = between_match.group(1).lower()
        val1 = between_match.group(2)
        val2 = between_match.group(3)
        
        # Map column names to database column names
        col_mapping = {
            'income': 'annual_income_k',
            'savings': 'estimated_savings_k',
            'spending': 'spending_score',
            'credit': 'credit_score',
            'loyalty': 'loyalty_years'
        }
        col = col_mapping.get(col, col)  # Default to original if not in mapping
        val1, val2 = sorted([int(val1), int(val2)])  # Ensure val1 <= val2
        conditions.append(f"{col} BETWEEN {val1} AND {val2}")
        
        # Build the final query
        where_clause = " AND ".join(conditions)
        return f"SELECT * FROM customers WHERE {where_clause}"
    
    # Handle count queries (supports both 'customer' and 'customers')
    count_match = re.search(
        r'count(?:\s+all)?(?:\s+the)?\s+((?:male|female|non-binary|nonbinary|other)\s+)?customers?(?:\s+where\s+(.+))?', 
        query, 
        re.IGNORECASE
    )
    
    if count_match:
        gender = (count_match.group(1) or '').strip()
        where_condition = count_match.group(2) or ''
        
        # Build where clause
        conditions = []
        if gender:
            conditions.append(f"LOWER(gender) = LOWER('{gender}')")
        if where_condition:
            conditions.append(parse_where_condition(where_condition))
            
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ''
        
        # Return the SQL to be executed by query_database
        sql = f"SELECT COUNT(*) as count FROM customers {where_clause}"
        
        # Return the SQL query to be executed by the query_database function
        return sql
    
    # Handle pattern: "list [all] [the] customers with [column] [operator] [value]"
    column_query_match = re.search(
        r'list(?:\s+all)?(?:\s+the)?\s+((?:male|female|non-binary|nonbinary|other)\s+)?customers\s+with\s+(' + column_pattern + r')\s+(less than|greater than|less than or equal to|greater than or equal to|not equal to|not equal|!=|<=|>=|<|>|=|is|:)\s*([^\s]+)(?:\s|$)', 
        query, 
        re.IGNORECASE
    )
    
    # Special case for simple age queries
    if not column_query_match:
        age_match = re.search(
            r'list(?:\s+all)?(?:\s+the)?\s+customers\s+whose\s+age\s+is\s+(\d+)',
            query,
            re.IGNORECASE
        )
        if age_match:
            age = age_match.group(1)
            return {
                "type": "table",
                "query": query,
                "sql": f"SELECT * FROM customers WHERE age = {age}",
                "columns": ["customerid", "gender", "age", "annual_income_k", "spending_score", "age_group", "estimated_savings_k", "credit_score", "loyalty_years", "preferred_category"]
            }
    
    if not column_query_match:
        # Handle pattern: "list [all] [the] customers whose [column] [operator] [value]"
        column_query_match = re.search(
            r'list(?:\s+all)?(?:\s+the)?\s+((?:male|female|non-binary|nonbinary|other)\s+)?customers\s+whose\s+(' + column_pattern + r')\s+(?:is\s+)?(less than|greater than|less than or equal to|greater than or equal to|not equal to|not equal|!=|<=|>=|<|>|=|is|:)?\s*([^\s]+)(?:\s|$)', 
            query, 
            re.IGNORECASE
        )
    
    if not column_query_match:
        # Alternative pattern without "the"
        column_query_match = re.search(
            r'list(?:\s+of)?\s+((?:male|female|non-binary|nonbinary|other)\s+)?customers\s+with\s+(' + column_pattern + r')\s+(less than|greater than|less than or equal to|greater than or equal to|not equal to|not equal|!=|<=|>=|<|>|=|is|:)\s*([^\s]+)(?:\s|$)', 
            query, 
            re.IGNORECASE
        )
    
    if column_query_match:
        gender = column_query_match.group(1) or ''
        column_name = column_query_match.group(2).strip().lower().replace(' ', '_')
        operator = (column_query_match.group(3) or '=').lower().strip()
        column_value = column_query_match.group(4).strip("'\"")
        
        # Convert operator aliases to SQL operators
        operator_map = {
            'is': '=',
            '=': '=',
            ':': '=',
            'less than': '<',
            'greater than': '>',
            'less than or equal to': '<=',
            'greater than or equal to': '>=',
            'not equal to': '!=',
            'not equal': '!=',
            '!=': '!='
        }
        sql_operator = operator_map.get(operator, '=')
        
        # Handle different column types
        numeric_columns = ['age', 'spending_score', 'annual_income_k', 'credit_score', 'loyalty_years']
        text_columns = ['gender', 'preferred_category', 'age_group']
        
        conditions = []
        
        # Add gender condition if specified
        if gender:
            conditions.append(f"LOWER(gender) = LOWER('{gender.strip()}')")
        
        # Map operator aliases to SQL operators
        operator_map = {
            'is': '=',
            '=': '=',
            ':': '=',
            'less than': '<',
            'greater than': '>',
            'less than or equal to': '<=',
            'greater than or equal to': '>=',
            'not equal to': '!=',
            'not equal': '!=',
            '!=': '!='
        }
        
        # Get the SQL operator, default to '=' if not found
        sql_operator = operator_map.get(operator, '=')
        
        # Handle the column condition
        if column_name in numeric_columns:
            try:
                # Try to convert to number
                value = float(column_value) if '.' in column_value else int(column_value)
                conditions.append(f"{column_name} {sql_operator} {value}")
            except ValueError:
                # If conversion fails, treat as text with LIKE
                conditions.append(f"{column_name}::TEXT ILIKE '%{column_value}%'")
        elif column_name in text_columns:
            # For text columns, only allow equality or LIKE operations
            if sql_operator in ['<', '>', '<=', '>=']:
                conditions.append(f"LOWER({column_name}) LIKE LOWER('%{column_value}%')")
            else:
                conditions.append(f"LOWER({column_name}) = LOWER('{column_value}')")
        else:
            # For unknown columns, try to guess the type
            if column_value.replace('.', '').isdigit():
                conditions.append(f"{column_name} {sql_operator} {column_value}")
            else:
                if sql_operator in ['<', '>', '<=', '>=']:
                    conditions.append(f"{column_name}::TEXT ILIKE '%{column_value}%'")
                else:
                    conditions.append(f"LOWER({column_name}) = LOWER('{column_value}')")
        
        where_clause = " AND ".join(conditions)
        return f"SELECT * FROM customers WHERE {where_clause}"
    
    if list_customers_match:
        gender = list_customers_match.group(1) or ''
        condition = list_customers_match.group(2) or ''
        gender = gender.strip().lower() if gender else ''
        
        print(f"[DEBUG] Processing 'list customers' query with gender: '{gender}', condition: '{condition}'")
        
        conditions = []
        
        # Add gender condition if specified
        if gender:
            conditions.append(f"LOWER(gender) = LOWER('{gender}')")
        
        # Handle field conditions in the format "[field] is [value]"
        field_conditions = re.findall(r'(\w+)\s*(?:is|=|:)\s*(\d+|\w+)', condition, re.IGNORECASE)
        for field, value in field_conditions:
            field_lower = field.lower()
            # Handle different field types
            if field_lower in ['age', 'spending_score', 'annual_income_k', 'credit_score', 'loyalty_years']:
                conditions.append(f"{field_lower} = {value}")
            elif field_lower == 'gender':
                conditions.append(f"LOWER({field_lower}) = LOWER('{value}')")
            elif field_lower in ['preferred_category', 'age_group']:
                conditions.append(f"LOWER({field_lower}) = LOWER('{value}')")
        
        # Also check for specific patterns that might have been missed
        if not field_conditions:
            # Handle age condition
            age_match = re.search(r'age\s*(?:is|=|:)\s*(\d+)', condition, re.IGNORECASE)
            if age_match:
                age = age_match.group(1)
                conditions.append(f"age = {age}")
            
            # Handle spending score condition
            spending_match = re.search(r'spending[\s_-]?score\s*(?:is|=|:)\s*(\d+)', condition, re.IGNORECASE)
            if spending_match:
                score = spending_match.group(1)
                conditions.append(f"spending_score = {score}")
        
        # Build the WHERE clause
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return f"SELECT * FROM customers WHERE {where_clause}"
    
    # Handle specific pattern: "list all the columns of customer whose [gender] [and] preferred_category X"
    list_columns_match = re.search(
        r'list\s+all\s+the\s+columns\s+of\s+customer\s+'
        r'whose\s+'
        r'(?:(male|female|non-binary|nonbinary|other)\s+)?(?:and\s+)?'
        r'(?:preferred[_-]?category\s*(?:is\s+)?)?([\w\s-]+)', 
        query, re.IGNORECASE
    )
    
    if list_columns_match:
        gender = list_columns_match.group(1) or ''
        category = list_columns_match.group(2).strip()
        print(f"[DEBUG] Processing list columns for gender: {gender}, preferred_category: {category}")
        
        conditions = []
        if gender:
            conditions.append(f"LOWER(gender) = LOWER('{gender}')")
        if category:
            conditions.append(f"LOWER(preferred_category) = LOWER('{category}')")
            
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return f"SELECT * FROM customers WHERE {where_clause}"
        
    # Process spending score conditions
    def process_spending_score_conditions():
        if 'spending' in query or 'score' in query:
            print(f"[DEBUG] Processing spending/score in query: {query}")
            
            # Pattern 1: "spending_score [operator] X" or "spending score [operator] X"
            pattern1 = r'spending[_-]?score\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)?\s*([^\s\'\"]+|\'.*?\'|\d+(?:\.\d+)?)'
            spending_score_match = re.search(pattern1, query, re.IGNORECASE)
            print(f"[DEBUG] Pattern 1 match: {spending_score_match is not None}")
            
            # Pattern 2: "[operator] X (in|for) spending (score)?"
            if not spending_score_match:
                pattern2 = r'(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)\s*(\d+)\s*(?:and up|or more|plus)?\s*(?:in|for)?\s*spending(?:\s*score)?'
                spending_score_match = re.search(pattern2, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2 match: {spending_score_match is not None}")
            
            # Pattern 2b: "spending score [operator] X" (with operator after 'score')
            if not spending_score_match:
                pattern2b = r'spending[_-]?score\s+(?:is\s+)?(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)\s+(\d+)'
                spending_score_match = re.search(pattern2b, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2b match: {spending_score_match is not None}")
            
            # Pattern 2c: "spending score greater/more than X"
            if not spending_score_match:
                pattern2c = r'spending[_-]?score\s+(?:is\s+)?(greater than|more than|less than|at least|at most|over|under|above|below|exactly)\s+(\d+)'
                spending_score_match = re.search(pattern2c, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2c match: {spending_score_match is not None}")
            
            # Pattern 2d: "where spending score > X"
            if not spending_score_match:
                pattern2d = r'where\s+spending[_-]?score\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)\s*(\d+)'
                spending_score_match = re.search(pattern2d, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2d match: {spending_score_match is not None}")
            
            # Pattern 2e: "with spending score > X"
            if not spending_score_match:
                pattern2e = r'with\s+spending[_-]?score\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)\s*(\d+)'
                spending_score_match = re.search(pattern2e, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2e match: {spending_score_match is not None}")
            
            # Pattern 2f: General pattern for "spending_score greater than X" (with underscore)
            if not spending_score_match:
                pattern2f = r'spending_score\s+(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)\s+(\d+)'
                spending_score_match = re.search(pattern2f, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2f match: {spending_score_match is not None}")
            
            # Pattern 2g: Pattern for "spending score > X" (with space)
            if not spending_score_match:
                pattern2g = r'spending score\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)\s*(\d+)'
                spending_score_match = re.search(pattern2g, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2g match: {spending_score_match is not None}")
            
            # Pattern 2h: Pattern for "spending score is more than X"
            if not spending_score_match:
                pattern2h = r'spending score (?:is )?(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly) (\d+)'
                spending_score_match = re.search(pattern2h, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 2h match: {spending_score_match is not None}")
            
            # Pattern 3: "high/low spending" patterns
            if not spending_score_match:
                pattern3 = r'(high|low)(?:\s*|-)spending(?:\s*score)?'
                spending_score_match = re.search(pattern3, query, re.IGNORECASE)
                print(f"[DEBUG] Pattern 3 match: {spending_score_match is not None}")
                
                if spending_score_match:
                    print(f"[DEBUG] Found high/low spending pattern")
                    is_high = spending_score_match.group(1).lower() == 'high'
                    op = '>' if is_high else '<'
                    value = '70' if is_high else '30'
                    condition = f"spending_score {op} {value}"
                    where_conditions.append(condition)
                    processed_conditions.add('spending_score')
                    print(f"[DEBUG] Added spending score condition: {condition}")
                    return True
            
            # Handle numeric comparisons for patterns 1 and 2
            if spending_score_match and (spending_score_match.groups()[1] if spending_score_match.groups() else False):
                print(f"[DEBUG] Processing numeric comparison")
                op = spending_score_match.groups()[0] or '>='
                value = spending_score_match.groups()[1]
                
                op_map = {
                    '>': '>', '<': '<', '>=': '>=', '<=': '<=', '=': '=', '!=': '!=',
                    'greater than': '>', 'less than': '<', 'more than': '>',
                    'over': '>', 'under': '<', 'below': '<', 'above': '>',
                    'at least': '>=', 'at most': '<=', 'exactly': '='
                }
                op = op_map.get(op.lower() if op else '', '>=')
                
                condition = f"spending_score {op} {value}"
                where_conditions.append(condition)
                processed_conditions.add('spending_score')
                print(f"[DEBUG] Added spending score condition: {condition}")
                return True
        return False
    
    # 1. Check for explicit WHERE clause first (this takes highest precedence)
    where_match = re.search(r'\bwhere\s+(.+)', query, re.IGNORECASE)
    if where_match:
        try:
            where_clause = where_match.group(1).strip()
            print(f"[DEBUG] Found WHERE clause: {where_clause}")
            
            # Remove the WHERE part from the original query for further processing
            query = query[:where_match.start()].strip()
            
            # Handle direct column comparisons in the format "column operator value"
            direct_patterns = [
                # Numeric comparisons (credit_score > 500)
                (r'(credit_score|estimated_savings_k|annual_income_k|age|loyalty_years|spending_score)\s*(>|<|>=|<=|=|!=)\s*(\d+(?:\.\d+)?)', 
                 lambda m: (m.group(1), m.group(2), m.group(3))),
                # String comparisons (preferred_category = 'luxury')
                (r'(preferred_category|gender|age_group)\s*(=|!=|like|ilike)\s*[\'\"]([^\'\"]+)[\'\"]',
                 lambda m: (m.group(1), m.group(2), f"'{m.group(3)}'"))
            ]
            
            # Check for direct column comparisons in the WHERE clause
            matched = False
            for pattern, processor in direct_patterns:
                match = re.search(pattern, where_clause, re.IGNORECASE)
                if match:
                    try:
                        column, operator, value = processor(match)
                        condition = f"{column} {operator} {value}"
                        where_conditions.append(condition)
                        processed_conditions.add(column)
                        matched = True
                        print(f"[DEBUG] Added direct condition: {condition}")
                        break
                    except Exception as e:
                        print(f"[WARNING] Error processing direct pattern: {e}")
            
            # If no direct pattern matched, try the generic where clause parser
            if not matched:
                sql_condition, params = parse_where_clause(where_clause)
                where_conditions.append(sql_condition)
                print(f"[DEBUG] Added parsed WHERE condition: {sql_condition}")
                
        except Exception as e:
            print(f"[ERROR] Error processing WHERE clause: {str(e)}")
            raise
    
    # 2. Handle SELECT clause (what to return)
    if "show me" in query or "list" in query or "display" in query or "customer list" in query or not select:
        # Handle 'customer list where...' pattern
        if "customer list where" in query:
            select = ["*"]
        # Handle 'list customers with...' pattern
        elif "list customers with" in query:
            select = ["*"]
        # Extract what to show for other patterns
        elif "all" in query and ("show me" in query or "list" in query):
            select = ["*"]
        else:
            # Look for specific column mentions
            column_mentions = re.findall(r'\b(?:show me|list|display|customer list)\s+(?:the\s+)?(?:list of\s+)?(?:all\s+)?(.*?)(?:\s+where|\s+with|\s+group by|\s+order by|\s+limit|$)', query, re.IGNORECASE)
            if column_mentions:
                cols_text = column_mentions[0].strip()
                if cols_text.lower() in ["customers", "data", "results", ""]:
                    select = ["*"]
                else:
                    # Extract valid column names
                    valid_columns = ['customerid', 'gender', 'age', 'annual_income_k', 
                                   'spending_score', 'age_group', 'estimated_savings_k',
                                   'credit_score', 'loyalty_years', 'preferred_category']
                    mentioned_columns = [col.strip() for col in re.split(r'[,\s]+and\s+|,', cols_text) if col.strip()]
                    select = [col for col in mentioned_columns if any(vc.startswith(col.lower()) for vc in valid_columns)]
                    
                    # If no valid columns found, default to all
                    if not select:
                        select = ["*"]
            # Try to extract specific columns
            cols_match = re.search(r'(?:show me|list|display|customer list)\s+(?:the\s+)?(?:list of\s+)?(?:all\s+)?(.*?)(?:\s+where|\s+with|\s+group by|\s+order by|\s+limit|$)', query)
            if cols_match:
                cols_text = cols_match.group(1).strip()
                if cols_text not in ["customers", "data", "results", ""]:
                    select = [col.strip() for col in re.split(r'[,\s]+and\s+|,', cols_text) if col.strip()]
    
    # If no specific columns selected, default to all
    if not select:
        select = ["*"]
    
    # 2. Handle WHERE conditions from natural language patterns
    # Process spending score conditions if not already handled by WHERE clause
    if 'spending_score' not in processed_conditions:
        process_spending_score_conditions()
    
    # Handle direct column comparisons in the format "column operator value"
    direct_patterns = [
        # Numeric comparisons (credit_score > 500)
        (r'(credit_score|estimated_savings_k|annual_income_k|age|loyalty_years|spending_score)\s*(>|<|>=|<=|=|!=)\s*(\d+(?:\.\d+)?)', 
         lambda m: (m.group(1), m.group(2), m.group(3))),
        # String comparisons (preferred_category = 'luxury')
        (r'(preferred_category|gender|age_group)\s*(=|!=|like|ilike)\s*[\'\"]([^\'\"]+)[\'\"]',
         lambda m: (m.group(1), m.group(2), f"'{m.group(3).lower()}'" if m.group(1) == 'preferred_category' else f"'{m.group(3)}'"))
    ]
    
    # Check for direct column comparisons in the main query
    for pattern, processor in direct_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match and all(match.group(1).lower() not in cond.lower() for cond in where_conditions):
            try:
                column, operator, value = processor(match)
                condition = f"{column} {operator} {value}"
                where_conditions.append(condition)
                processed_conditions.add(column.lower())
                print(f"[DEBUG] Added direct condition from main query: {condition}")
            except Exception as e:
                print(f"[WARNING] Error processing direct pattern in main query: {e}")
    
    # Handle common natural language patterns for specific columns
    nl_patterns = [
        # Credit score patterns
        (r'(?:credit score|credit_score|credit)(?:\s+is)?\s*(>|>=|greater than or equal to|greater than|more than|above|over)\s*(\d+)', 
         lambda m: ("credit_score", ">" if m.group(1) in [">", "greater than", "more than", "above", "over"] else ">=", m.group(2))),
        (r'(?:credit score|credit_score|credit)(?:\s+is)?\s*(<|<=|less than or equal to|less than|below|under)\s*(\d+)', 
         lambda m: ("credit_score", "<" if m.group(1) in ["<", "less than", "below", "under"] else "<=", m.group(2))),
        (r'(?:credit score|credit_score|credit)(?:\s+is)?\s*(?:=|equal to)?\s*(\d+)',
         lambda m: ("credit_score", "=", m.group(1))),
         
        # Estimated savings patterns
        (r'(?:estimated savings|estimated_savings_k|savings)(?:\s+is)?\s*(>|>=|greater than or equal to|greater than|more than|above|over)\s*(\d+)', 
         lambda m: ("estimated_savings_k", ">" if m.group(1) in [">", "greater than", "more than", "above", "over"] else ">=", m.group(2))),
        (r'(\d+)\s*(?:k|K)?\s*(?:and up|or more|plus)?\s*(?:in|of)?\s*(?:savings|estimated savings)',
         lambda m: ("estimated_savings_k", ">=", m.group(1))),
         
        # Preferred category patterns (case-insensitive)
        (r'(?:preferred category|category|preferred_category)(?:\s+is|:)?\s*[\'\"]([^\'\"]+)[\'\"]',
         lambda m: ("LOWER(preferred_category)", "=", f"'{m.group(1).lower()}'"))
    ]
    
    for pattern, processor in nl_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            try:
                column, operator, value = processor(match)
                condition = f"{column} {operator} {value}"
                col_name = column.split('(')[0].lower()
                if condition not in where_conditions and col_name not in processed_conditions:
                    where_conditions.append(condition)
                    processed_conditions.add(col_name)
                    print(f"[DEBUG] Added NL pattern condition: {condition}")
            except Exception as e:
                print(f"[WARNING] Error processing NL pattern {pattern}: {e}")
    
    # Special handling for gender
    if 'gender' not in processed_conditions:
        gender_match = re.search(r'\b(female|male|woman|man|girl|boy)s?\b', query, re.IGNORECASE)
        if gender_match:
            gender = 'Female' if gender_match.group(1).lower() in ['female', 'woman', 'girl'] else 'Male'
            where_conditions.append(f"gender = '{gender}'")
            processed_conditions.add('gender')
            print(f"[DEBUG] Added gender condition: gender = '{gender}'")
    
    # estimated_savings_k conditions - handle multiple patterns
    savings_patterns = [
        # Pattern 1: "savings > X" or "savings over X"
        (r'(?:savings?|estimated savings?)\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)?\s*\$?(\d+(?:,\d+)*(?:\.\d+)?)(?:k|K)?(?:\s*dollars?)?'),
        # Pattern 2: "X+ (in|of) savings"
        (r'(\d+(?:\.\d+)?)\s*(?:k|K)?(?:\s*dollars?)?\s*(?:and up|or more|plus)?\s*(?:in|of)?\s*(?:savings?|estimated savings?)'),
        # Pattern 3: "more/less than X in savings"
        r'(?:more than|less than|over|under|above|below)\s*\$?(\d+(?:\.\d+)?)(?:k|K)?(?:\s*dollars?)?(?:\s+in|\s+of)?\s*(?:savings?|estimated savings?)'
    ]
    
    if not any('estimated_savings_k' in cond for cond in where_conditions):
        for pattern in savings_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                groups = [g for g in match.groups() if g is not None]
                if len(groups) >= 2:
                    op = groups[0].lower() if groups[0] else '>='
                    value = groups[1].replace(',', '')
                else:
                    # For patterns with single group (like pattern 2 and 3)
                    op = '>=' if 'more' in query.lower() or 'over' in query.lower() or 'above' in query.lower() else '<='
                    value = groups[0].replace(',', '')
                
                # Handle 'k' suffix (e.g., 10k = 10)
                if 'k' in query.lower() and 'k' not in value and '.' not in value and not any(x in query.lower() for x in ['dollar', 'saving']):
                    value = f"{float(value):.1f}"
                
                op_map = {
                    '>': '>', '<': '<', '>=': '>=', '<=': '<=', '=': '=', '!=': '!=',
                    'greater than': '>', 'less than': '<', 'more than': '>',
                    'over': '>', 'under': '<', 'below': '<', 'above': '>',
                    'at least': '>=', 'at most': '<=', 'exactly': '=',
                    'greater': '>', 'more': '>', 'less': '<'
                }
                op = op_map.get(op, '>=')
                add_condition('estimated_savings_k', op, value)
                break
    
    # Income conditions - handle multiple patterns
    income_patterns = [
        # Pattern 1: "income [operator] X"
        (r'(?:annual_?)?income_?k?\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)?\s*\$?(\d+(?:,\d+)*(?:\.\d+)?)(?:k|K)?(?:\s*dollars?)?', 'annual_income_k'),
        # Pattern 2: "[operator] X (k|dollars) income"
        (r'(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)?\s*\$?(\d+(?:,\d+)*(?:\.\d+)?)(?:k|K)?(?:\s*dollars?)?\s*(?:per year|per annum|annual)?\s*(?:income|salary)', 'annual_income_k'),
        # Pattern 3: "income is [operator] X"
        (r'(?:annual_?)?income_?k?\s*(?:is|was|are|were)?\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)?\s*\$?(\d+(?:,\d+)*(?:\.\d+)?)(?:k|K)?(?:\s*dollars?)?', 'annual_income_k')
    ]
    
    # Only process if we haven't already added an income condition
    if not any('annual_income_k' in cond for cond in where_conditions):
        for pattern, field in income_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                groups = [g for g in match.groups() if g is not None]
                if len(groups) >= 2:
                    op = groups[0].lower() if groups[0] else '>='
                    value = groups[1].replace(',', '')
                    
                    # Handle 'k' suffix (e.g., 15k = 15000)
                    if 'k' in query.lower() and 'k' not in value and '.' not in value and not any(x in query.lower() for x in ['dollar', 'income']):
                        value = f"{float(value) * 1:.1f}"  # Keep as k for annual_income_k column
                    
                    op_map = {
                        '>': '>', '<': '<', '>=': '>=', '<=': '<=', '=': '=', '!=': '!=',
                        'greater than': '>', 'less than': '<', 'more than': '>',
                        'over': '>', 'under': '<', 'below': '<', 'above': '>',
                        'at least': '>=', 'at most': '<=', 'exactly': '=',
                        'greater': '>', 'more': '>', 'less': '<'
                    }
                    op = op_map.get(op, '>=')
                    
                    # Add the condition
                    condition = f"{field} {op} {value}"
                    if condition not in where_conditions:
                        where_conditions.append(condition)
                        print(f"[DEBUG] Added income condition: {condition}")
                    break
    
    # preferred_category conditions - handle multiple patterns
    category_mappings = {
        'luxury': ['luxury', 'premium', 'high-end', 'high end', 'upscale', 'deluxe'],
        'budget': ['budget', 'economy', 'low-cost', 'low cost', 'affordable', 'cheap'],
        'standard': ['standard', 'regular', 'normal', 'basic']
    }
    
    if not any('preferred_category' in cond for cond in where_conditions):
        for category, terms in category_mappings.items():
            if any(f' {term} ' in f' {query.lower()} ' for term in terms):
                add_condition('preferred_category', '=', category.capitalize(), is_text=True)
                break
    
    # age_group conditions - handle multiple patterns
    age_group_patterns = [
        (r'(?:age[-\s]?group|age group)\s*(?:is\s*|of\s*)?(\d+\s*[-–]\s*\d+)'),  # Matches 'age group 18-25' or 'age-group 18-25'
        (r'(?:age|aged|in the age of|in their)\s*(\d+\s*[-–]\s*\d+)\s*(?:years?|yrs?)?'),  # Matches 'age 18-25' or 'in the age of 18-25'
        (r'(\d+\s*[-–]\s*\d+)\s*(?:year old|years? old|yrs? old|year[- ]?olds?)')  # Matches '18-25 year olds' or '18-25 years old'
    ]
    
    if not any('age_group' in cond for cond in where_conditions):
        for pattern in age_group_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                age_group = match.group(1).replace(' ', '')  # Remove any spaces in the age group
                add_condition('age_group', '=', age_group)
                break
    
    # 3. Handle aggregations
    agg_map = {
        'average': 'AVG', 'avg': 'AVG', 'mean': 'AVG',
        'count': 'COUNT', 'number of': 'COUNT', 'total': 'COUNT',
        'sum': 'SUM', 'total of': 'SUM',
        'minimum': 'MIN', 'min': 'MIN', 'lowest': 'MIN',
        'maximum': 'MAX', 'max': 'MAX', 'highest': 'MAX',
        'max age': 'MAX(age)', 'maximum age': 'MAX(age)',
        'min age': 'MIN(age)', 'minimum age': 'MIN(age)',
        'average age': 'AVG(age)'
    }
    
    # Special handling for specific aggregations
    special_agg_map = {
        # Loyalty years aggregations
        'max loyalty years': 'MAX(loyalty_years) AS max_loyalty_years',
        'maximum loyalty years': 'MAX(loyalty_years) AS max_loyalty_years',
        'min loyalty years': 'MIN(loyalty_years) AS min_loyalty_years',
        'minimum loyalty years': 'MIN(loyalty_years) AS min_loyalty_years',
        'average loyalty years': 'AVG(loyalty_years) AS avg_loyalty_years',
        'avg loyalty years': 'AVG(loyalty_years) AS avg_loyalty_years',
        
        # Age aggregations
        'sum of all the age of customer': 'SUM(age) AS total_age',
        'total age of all customers': 'SUM(age) AS total_age',
        'sum of customer ages': 'SUM(age) AS total_age',
        'sum of ages': 'SUM(age) AS total_age'
    }
    
    # Handle gender-based aggregations (max/min age of male/female)
    gender_aggregations = [
        ('max age of female', 'MAX(age) AS max_age', 'Female'),
        ('min age of female', 'MIN(age) AS min_age', 'Female'),
        ('max age of male', 'MAX(age) AS max_age', 'Male'),
        ('min age of male', 'MIN(age) AS min_age', 'Male'),
        ('average age of female', 'AVG(age) AS avg_age', 'Female'),
        ('average age of male', 'AVG(age) AS avg_age', 'Male'),
        ('count of females', 'COUNT(*) AS female_count', 'Female'),
        ('count of males', 'COUNT(*) AS male_count', 'Male')
    ]
    
    for pattern, agg_func, gender in gender_aggregations:
        if pattern in query:
            select = [agg_func]
            if not any('gender' in cond for cond in where_conditions):
                where_conditions.append(f"gender = '{gender}'")
            break
    else:
        # First check for special aggregations (loyalty years, sum of ages, etc.)
        for term, sql_expr in special_agg_map.items():
            if term in query.lower():
                select = [sql_expr]
                break
        else:
            # Check for other aggregations if no loyalty aggregation was found
            for term, func in agg_map.items():
                if term in query:
                    # Skip if we already handled this as a special case
                    if any(x in query for x in ['max age of female', 'min age of female']):
                        break
                        
                    # Find what's being aggregated
                    agg_match = re.search(fr'{term}(?:\s+of|\s+the|\s+for|\s+in)?\s+([\w\s]+?)(?:\s+where|\s+group by|\s+order by|\s+limit|$)', query)
                    if agg_match:
                        column = agg_match.group(1).strip()
                        if column in ['customers', 'records', 'rows', 'entries']:
                            select = [f"{func} AS {term}_count"]
                        elif 'age' in term:  # For age-specific aggregations
                            select = [f"{func} AS {term.replace(' ', '_')}"]
                        else:
                            select = [f"{func}({column}) AS {term}_{column}"]
                        break
                    
                # Find what's being aggregated
                agg_match = re.search(fr'{term}(?:\s+of|\s+the|\s+for|\s+in)?\s+([\w\s]+?)(?:\s+where|\s+group by|\s+order by|\s+limit|$)', query)
                if agg_match:
                    column = agg_match.group(1).strip()
                    if column in ['customers', 'records', 'rows', 'entries']:
                        select = [f"{func} AS {term}_count"]
                    elif 'age' in term:  # For age-specific aggregations
                        select = [f"{func} AS {term.replace(' ', '_')}"]
                    else:
                        select = [f"{func}({column}) AS {term}_{column}"]
                    break
    
    # 4. Handle grouping
    if "group by" in query or "by" in query.split()[-2:]:
        group_match = re.search(r'group by\s+([\w\s,]+?)(?:\s+having|\s+order by|\s+limit|$)', query)
        if not group_match and "by" in query.split()[-2:]:
            # Try to extract after 'by' at the end
            by_parts = query.split(' by ')
            if len(by_parts) > 1:
                group_terms = by_parts[-1].split()
                group_by = [term for term in group_terms if term not in ['and', 'with', 'then', 'order', 'limit']]
        elif group_match:
            group_by = [g.strip() for g in group_match.group(1).split(',')]
    
    # 5. Handle loyalty years conditions
    loyalty_phrases = [
        # Pattern 1: "loyalty year[s] [operator] X" - convert to loyalty_years
        (r'loyalty\s*years?\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|at least|at most|exactly)?\s*(\d+)', 'loyalty_years'),
        # Pattern 2: "[operator] X year[s] [of] loyalty" - convert to loyalty_years
        (r'(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|at least|at most|exactly)?\s*(\d+)\s*(?:year|yr)s?\s*(?:of\s*)?loyalty', 'loyalty_years'),
        # Pattern 3: "customer for X year[s]" - convert to loyalty_years
        (r'customer\s*for\s*(\d+)\s*(?:year|yr)s?', 'loyalty_years'),
        # Pattern 4: "with/has X+ year[s] [of] loyalty" - convert to loyalty_years
        (r'(?:with|has|have)\s+(?:a\s+)?(\d+)(?:\+)?\s*(?:year|yr)s?\s*(?:of\s*)?loyalty', 'loyalty_years'),
        # Pattern 5: "where loyalty year[s] [is] [operator] X" - convert to loyalty_years
        (r'where\s+loyalty\s*years?\s*(?:is\s*)?(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|at least|at most|exactly)?\s*(\d+)', 'loyalty_years'),
        # Pattern 6: "loyalty_years [operator] X" - direct match with underscore
        (r'loyalty_years\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|below|above|at least|at most|exactly)?\s*(\d+)', 'loyalty_years')
    ]
    
    # Only process if we haven't already added a loyalty condition
    if not any('loyalty_years' in w for w in where_conditions):
        for pattern, field in loyalty_phrases:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                operator_map = {
                    '>': '>', '<': '<', '>=': '>=', '<=': '<=', '=': '=', '!=': '!=',
                    'greater than': '>', 'more than': '>', 'over': '>', 'above': '>',
                    'less than': '<', 'under': '<', 'below': '<',
                    'at least': '>=', 'at most': '<=', 'exactly': '=', 'minimum': '>=', 'maximum': '<=',
                    'greater': '>', 'more': '>', 'less': '<'
                }
                
                # Debug: Print match groups
                print(f"[DEBUG] Match groups: {match.groups()}")
                
                # Check for comparison operators in the match
                groups = [g for g in match.groups() if g is not None]
                if len(groups) >= 2:
                    # If we have an operator and value
                    if groups[0].lower() in operator_map:
                        op = operator_map[groups[0].lower()]
                        value = groups[1]
                    else:
                        # If first group is a number (no operator specified)
                        op = '>='
                        value = groups[0]
                else:
                    # Single group match (just a number)
                    op = '>='
                    value = groups[0]
                
                # Add the condition to where_conditions
                condition = f"{field} {op} {value}"
                if condition not in where_conditions:  # Avoid duplicates
                    where_conditions.append(condition)
                    print(f"[DEBUG] Added condition: {condition}")
                break
    
    # 6. Handle ordering
    if any(term in query for term in ['sort by', 'order by', 'sorted by', 'ordered by']):
        order_match = re.search(r'(?:sort|order)(?:ed|ing)?\s+by\s+([\w\s,]+?)(?:\s+(asc|desc|ascending|descending))?(?:\s+order by|\s+limit|$)', query, re.IGNORECASE)
        if order_match:
            order_cols = order_match.group(1).split(',')
            direction = (order_match.group(2) or 'asc').lower()
            direction = 'DESC' if 'desc' in direction else 'ASC'
            order_by = [f"{col.strip()} {direction}" for col in order_cols]
    
    # Special case for top/bottom N queries
    top_match = re.search(r'(?:top|bottom|first|last)\s+(\d+)(?:\s+(?:most|least))?\s*(?:by|for)?\s*([\w\s]+)?(?:\s+customers|\s+users)?', query, re.IGNORECASE)
    if not top_match:
        # Alternative pattern for 'top N by X'
        top_match = re.search(r'(top|bottom)\s+(\d+)\s+(highest|lowest|best|worst)?\s*([\w\s]+)?', query, re.IGNORECASE)
        if top_match:
            position, num, order, metric = top_match.groups()
            limit = int(num)
            if metric:
                metric = metric.strip()
                if order in ['highest', 'best'] or position == 'top':
                    order_by = [f"{metric} DESC"]
                else:
                    order_by = [f"{metric} ASC"]
    else:
        # Handle the first pattern
        num, _, metric = top_match.groups()
        limit = int(num)
        if metric:
            metric = metric.strip()
            order_by = [f"{metric} DESC"]
    
    # 6. Build the final query
    if not select:
        select = ["*"]
    
    # Debug: Print the conditions
    print(f"\n[DEBUG] WHERE conditions: {where_conditions}")
    print(f"[DEBUG] SELECT: {select}")
    print(f"[DEBUG] FROM: {from_table}")
    
    # If we have a direct SQL condition from WHERE clause, use that
    if 'direct_sql_condition' in locals():
        where_conditions.insert(0, direct_sql_condition)
    
    # Start building the SQL query
    sql = f"SELECT {'DISTINCT ' if is_distinct else ''}"
    sql += ", ".join(select)
    sql += f" FROM {from_table}"
    
    # Add WHERE conditions if any
    if where_conditions:
        sql += " WHERE " + " AND ".join(where_conditions)
    
    # Add GROUP BY if specified
    if group_by:
        sql += " GROUP BY " + ", ".join(group_by)
    
    # Add HAVING clause if specified
    if having_conditions:
        sql += " HAVING " + " AND ".join(having_conditions)
    
    # Add ORDER BY if specified
    if order_by:
        sql += " ORDER BY " + ", ".join(order_by)
    
    # Add LIMIT if specified
    if limit is not None:
        sql += f" LIMIT {limit}"
    
    print(f"[DEBUG] Generated SQL: {sql}")
    return sql

def normalize_gender(term):
    """Normalize gender terms to 'Male' or 'Female'"""
    if term.lower() in ['male', 'males', 'men']:
        return 'Male'
    return 'Female'

def parse_age_condition(query):
    """
    Parse age conditions from the query and return a tuple of (sql_condition, params)
    
    Handles various age-related patterns:
    - Exact age: 'age is 25', 'age = 30'
    - Range: 'age between 20 and 30', 'age 20-30', 'age from 20 to 30'
    - Comparisons: 'age > 21', 'age less than 18', 'age under 18', 'age over 65'
    - Natural language: 'senior citizens', 'young adults', 'middle-aged'
    - Multiple conditions: 'age between 30 and 50 and high income'
    """
    query_lower = query.lower()
    conditions = []
    params = {}
    
    # 1. Handle 'same age' queries first as they're special cases
    if any(term in query_lower for term in ['same age', 'equal age', 'peers']):
        return "SAME_AGE_QUERY", {}
    
    # 2. Handle exact age matches
    exact_age = re.search(r'age\s*(?:is|=|:)\s*(\d+)', query_lower)
    if exact_age:
        age = int(exact_age.group(1))
        conditions.append("age = %(age)s")
        params['age'] = age
        return " AND ".join(conditions), params
    
    # 3. Handle age ranges (between X and Y)
    range_matches = [
        re.search(r'age\s+between\s+(\d+)\s+and\s+(\d+)', query_lower),
        re.search(r'age\s+(\d+)\s*-\s*(\d+)', query_lower),
        re.search(r'age\s+from\s+(\d+)\s+to\s+(\d+)', query_lower),
        re.search(r'(\d+)\s*to\s*(\d+)\s*years?', query_lower)
    ]
    
    for match in filter(None, range_matches):
        min_age, max_age = sorted(map(int, match.groups()))
        conditions.append("age BETWEEN %(min_age)s AND %(max_age)s")
        params.update({'min_age': min_age, 'max_age': max_age})
        return " AND ".join(conditions), params
    
    # 4. Handle comparison operators
    comparisons = [
        # Age at least/above/over/greater than
        (r'age\s*(>=|greater than or equal to|at least|minimum of|minimum|is greater than or equal to|is at least)\s*(\d+)', '>='),
        (r'age\s*(>|greater than|more than|over|above|is greater than|is more than|is over|is above)\s*(\d+)', '>'),
        # Age at most/below/under/less than
        (r'age\s*(<=|less than or equal to|at most|maximum of|maximum|is less than or equal to|is at most)\s*(\d+)', '<='),
        (r'age\s*(<|less than|under|below|is less than|is under|is below)\s*(\d+)', '<'),
        # Age equals
        (r'age\s*(?:=|is|is equal to|equals)\s*(\d+)', '='),
        # Standalone age comparisons
        (r'(\d+)\s*(and above|or more|and higher|plus|\+)(?:\s*years?)?(?:\s*old)?', '>='),
        (r'(\d+)\s*(and below|or less|and lower|minus|\-)(?:\s*years?)?(?:\s*old)?', '<=')
    ]
    
    for pattern, operator in comparisons:
        match = re.search(pattern, query_lower)
        if match:
            try:
                print(f"[DEBUG] Matched pattern '{pattern}' with operator '{operator}'")
                # Try to extract the number from the match
                age_groups = [g for g in match.groups() if g and g.replace('.', '').isdigit()]
                if age_groups:
                    age = int(age_groups[0])
                    print(f"[DEBUG] Extracted age: {age}")
                    # Create a safe parameter name without special characters
                    op_map = {'>=': 'gte', '<=': 'lte', '>': 'gt', '<': 'lt', '=': 'eq', '!=': 'ne'}
                    op_safe = op_map.get(operator, operator)
                    param_name = f"age_{op_safe}_{abs(hash(str(age))) % 1000}"  # Safe param name
                    condition = f"age {operator} %({param_name})s"
                    print(f"[DEBUG] Generated condition: {condition} with param: {param_name} = {age}")
                    conditions.append(condition)
                    params[param_name] = age
                    return " AND ".join(conditions), params
            except (ValueError, IndexError):
                continue
    
    # 5. Handle natural language age groups
    if any(term in query_lower for term in ['senior', 'elderly', 'retired', 'pensioner']):
        conditions.append("age >= 65")
    elif any(term in query_lower for term in ['middle age', 'middle-age', 'middleaged', 'middle age']):
        conditions.append("age BETWEEN 40 AND 64")
    elif any(term in query_lower for term in ['young adult', 'young adults', 'young adult']):
        conditions.append("age BETWEEN 18 AND 39")
    elif any(term in query_lower for term in ['teen', 'teenager', 'teenagers']):
        conditions.append("age BETWEEN 13 AND 19")
    elif any(term in query_lower for term in ['child', 'children', 'kid', 'kids']):
        conditions.append("age < 13")
    
    # 6. Handle 'age group' queries (only if no other conditions were found)
    if not conditions:
        age_group_match = re.search(r'age\s*group\s*(?:is|=|:)?\s*([\w\s-]+)', query_lower)
        if age_group_match:
            age_group = age_group_match.group(1).strip().lower()
            if 'senior' in age_group:
                return "age_group = '65+'", {}
            elif 'middle' in age_group:
                return "age_group = '40-64'", {}
            elif 'young' in age_group or 'adult' in age_group:
                return "age_group = '18-39'", {}
            elif 'teen' in age_group:
                return "age_group = '13-19'", {}
            elif 'child' in age_group or 'kid' in age_group:
                return "age < 13", {}
    
    # 7. Handle standalone age mentions (e.g., '21 and above')
    standalone_age = re.search(r'^(\d+)\s*(?:and above|and older|\+)?$', query_lower.strip())
    if standalone_age:
        age = int(standalone_age.group(1))
        param_name = f"age_ge_{age}"
        conditions.append(f"age >= %({param_name})s")
        params[param_name] = age
    
    return (" AND ".join(conditions), params) if conditions else (None, {})
    
    # Income filters - handle income separately from age
    if any(term in query.lower() for term in ["income", "salary", "earning"]):
        # More specific patterns for income
        income_patterns = [
            # Exact match pattern (e.g., 'income is 48' or 'income = 48')
            (r'(?:income|salary|earning).*?(?:is|equals?|=)\s*(\d+)', 'annual_income_k = {}'),
            # Less than pattern
            (r'(?:income|salary|earning).*?(\d+).*?(?:less than|below|under|<)', 'annual_income_k < {}'),
            # Greater than pattern
            (r'(?:income|salary|earning).*?(\d+).*?(?:more than|above|over|>)', 'annual_income_k > {}'),
            # Between pattern
            (r'(?:income|salary|earning).*?between (\d+).*and (\d+)', 'annual_income_k BETWEEN {} AND {}'),
            # Default pattern (if no comparison is specified, assume exact match)
            (r'(?:income|salary|earning).*?(\d+)', 'annual_income_k = {}')
        ]
        
        # Only process if we don't already have an income filter
        if not any('annual_income_k' in w for w in where):
            for pattern, template in income_patterns:
                match = re.search(pattern, query.lower())
                if match:
                    values = [int(g) for g in match.groups() if g is not None]
                    if len(values) == 1:
                        where.append(template.format(*values))
                    elif len(values) == 2:
                        # For between, make sure numbers are in order
                        low, high = sorted(values)
                        where.append(template.format(low, high))
                    break
    
    
    # Category filters
    if any(term in query for term in ["fashion", "clothing", "apparel"]):
        where.append("preferred_category = 'Fashion'")
    elif any(term in query for term in ["electronics", "gadgets", "tech"]):
        where.append("preferred_category = 'Electronics'")
    elif any(term in query for term in ["luxury", "premium", "high-end"]):
        where.append("preferred_category = 'Luxury'")
    elif any(term in query for term in ["budget", "economy", "saving"]):
        where.append("preferred_category = 'Budget'")
    
    # Gender filters
    if "female" in query.lower() or "woman" in query.lower() or "women" in query.lower() or "females" in query.lower():
        where.append("gender = 'Female'")
    elif "male" in query.lower() or "man" in query.lower() or "men" in query.lower() or "males" in query.lower():
        where.append("gender = 'Male'")
        
    # Loyalty years filter - only add the > condition
    if 'greater than' in query and 'loyalty_years' not in added_conditions:
        # First remove any existing loyalty_years conditions to prevent duplicates
        where = [w for w in where if 'loyalty_years' not in w]
        
        # Now add our specific > condition
        gt_match = re.search(r'loyalty_years\s*>\s*(\d+)', query, re.IGNORECASE)
        if not gt_match:
            gt_match = re.search(r'(?:loyalty|member).*?(?:years?|yrs?)?\s*(?:greater\s*than|more\s*than|over|>)\s*(\d+)', query, re.IGNORECASE)
        
        if gt_match:
            years = int(gt_match.group(1))
            where.append(f"loyalty_years > {years}")
            added_conditions.add('loyalty_years')
    
    # Sorting
    if "sort by" in query or "order by" in query or any(term in query for term in ["top ", "highest", "lowest"]):
        # Handle top N queries with ordering
        pass
    
    # Define loyalty patterns for parsing
    loyalty_phrases = [
        # Pattern 1: "loyalty year[s] [operator] X"
        (r'loyalty\s*years?\s*(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|at least|at most|exactly)?\s*(\d+)', 'loyalty_years'),
        # Pattern 2: "[operator] X year[s] [of] loyalty"
        (r'(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|at least|at most|exactly)?\s*(\d+)\s*(?:year|yr)s?\s*(?:of\s*)?loyalty', 'loyalty_years'),
        # Pattern 3: "customer for X year[s]"
        (r'customer\s*for\s*(\d+)\s*(?:year|yr)s?', 'loyalty_years'),
        # Pattern 4: "with/has X+ year[s] [of] loyalty"
        (r'(?:with|has|have)\s+(?:a\s+)?(\d+)(?:\+)?\s*(?:year|yr)s?\s*(?:of\s*)?loyalty', 'loyalty_years'),
        # Pattern 5: "where loyalty year[s] [is] [operator] X"
        (r'where\s+loyalty\s*years?\s*(?:is\s*)?(>|>=|<|<=|=|!=|greater than|less than|more than|over|under|at least|at most|exactly)?\s*(\d+)', 'loyalty_years')
    ]
    
    # Count query handling has been moved to the beginning of the function
    
    # Handle top/first N queries
    if "top " in query or "first " in query or "show " in query:
        if "top " in query or "first " in query:
            limit_match = re.search(r'(?:top|first)\s*(\d+)', query)
            if limit_match:
                limit = f"LIMIT {limit_match.group(1)}"
            
            # Set default ordering if not specified
            if not order_by:
                if any(term in query for term in ["spend", "spending"]):
                    order_by = "ORDER BY spending_score DESC"
                elif any(term in query for term in ["income", "salary", "earning"]):
                    order_by = "ORDER BY annual_income_k DESC"
                elif any(term in query for term in ["oldest", "senior"]) or "old" in query.split():
                    order_by = "ORDER BY age DESC"
                elif any(term in query for term in ["youngest", "junior"]):
                    order_by = "ORDER BY age ASC"
    
    # Process loyalty years conditions
    if not any('loyalty_years' in w for w in where_conditions):
        for pattern, field in loyalty_phrases:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                operator_map = {
                    '>': '>', '<': '<', '>=': '>=', '<=': '<=', '=': '=', '!=': '!=',
                    'greater than': '>', 'more than': '>', 'over': '>', 'above': '>',
                    'less than': '<', 'under': '<', 'below': '<',
                    'at least': '>=', 'at most': '<=', 'exactly': '=', 'minimum': '>=', 'maximum': '<=',
                    'greater': '>', 'more': '>', 'less': '<'
                }
                
                # Debug: Print match groups
                print(f"[DEBUG] Match groups: {match.groups()}")
                
                # Check for comparison operators in the match
                groups = [g for g in match.groups() if g is not None]
                if len(groups) >= 2:
                    # If we have an operator and value
                    if groups[0].lower() in operator_map:
                        op = operator_map[groups[0].lower()]
                        value = groups[1]
                    else:
                        # If first group is a number (no operator specified)
                        op = '>='
                        value = groups[0]
                else:
                    # Single group match (just a number)
                    op = '>='
                    value = groups[0]
                
                # Add the condition to where_conditions instead of where
                condition = f"{field} {op} {value}"
                if condition not in where_conditions:  # Avoid duplicates
                    where_conditions.append(condition)
                    print(f"[DEBUG] Added condition: {condition}")
                break
    
    # Handle customer ID lookup first (takes precedence over other filters)
    id_match = re.search(r'(?:id\s*(?:is|=|:)\s*|#|id\s*|customer\s*)(\d+)', query, re.IGNORECASE)
    if id_match and not any(term in query.lower() for term in ['age', 'income', 'spending', 'loyalty']):
        where = [f"customerid = {int(id_match.group(1))}"]
        limit = ""  # Remove any limit for exact ID lookups
    else:
        # Handle top/first/last/limit
        limit = ""  # No default limit
        
        # Check for 'last N' pattern
        last_match = re.search(r'last\s*(\d+)', query, re.IGNORECASE)
        if last_match:
            limit_num = int(last_match.group(1))
            limit = f"LIMIT {limit_num}"
            order_by = "ORDER BY customerid DESC"
        else:
            # Handle other limit patterns
            limit_match = re.search(r'(?:top|first|show|limit)\s*(\d+)', query, re.IGNORECASE)
            if limit_match:
                limit_num = int(limit_match.group(1))
                limit = f"LIMIT {limit_num}"
    
    # Remove any duplicate conditions and ensure only one loyalty_years condition exists
    unique_conditions = {}
    for condition in where:
        if 'loyalty_years' in condition:
            # Only keep the first loyalty_years condition we find
            if 'loyalty_years' not in unique_conditions:
                unique_conditions['loyalty_years'] = condition
        else:
            # For other conditions, just use the condition as the key to avoid duplicates
            unique_conditions[condition] = condition
    
    # Rebuild the where list with unique conditions
    where = list(unique_conditions.values())
    
    # Build the WHERE clause
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    
    # Special handling for ordering
    if not order_by:
        if "top " in query or "first " in query:
            order_by = "ORDER BY customerid ASC"
        elif "last " in query:
            order_by = "ORDER BY customerid DESC"
    
    # Build the final SQL
    sql = f"""
    {select}
    FROM customers
    {where_clause}
    {order_by}
    {limit}
    """.strip()  # Remove extra whitespace
    
    # For average queries, remove any LIMIT and ORDER BY as they don't make sense with a single value
    if "AVG(" in select:
        sql = sql.replace("LIMIT 10", "").replace("ORDER BY customerid ASC", "").replace("ORDER BY customerid DESC", "")
    
    # Clean up any double spaces and trim
    # Remove extra whitespace and return the SQL
    return ' '.join(sql.split())


def query_database(natural_query):
    """Execute a natural language query against the database."""
    print(f"\n[DEBUG] Processing query: {natural_query}")
    
    try:
        # Convert natural language to SQL
        query_result = parse_simple_query(natural_query)
        print(f"[DEBUG] Generated SQL: {query_result}")
        
        # Handle case where parse_simple_query returns a dictionary (for metrics like count)
        if isinstance(query_result, dict):
            if 'sql' in query_result:
                # Get parameters if they exist
                params = query_result.get('params', {})
                # Execute the SQL from the dictionary with parameters
                result = execute_query(query_result['sql'], params)
                
                # If it's a table query, return the data in the expected format
                if query_result.get('type') == 'table':
                    return {
                        "success": True,
                        "type": "table",
                        "query": natural_query,
                        "sql": query_result['sql'],
                        "data": result,
                        "count": len(result) if result else 0,
                        "label": query_result.get('label', 'Query Results')
                    }
                # If it's a metric query, update the value in the result
                elif query_result.get('type') == 'metric' and result and len(result) > 0:
                    count_value = result[0].get('count') or result[0].get(next(iter(result[0]))) if result else 0
                    return {
                        "success": True,
                        "type": "metric",
                        "label": query_result.get('label', 'Count'),
                        "value": int(count_value) if count_value is not None else 0,
                        "query": natural_query,
                        "sql": query_result['sql']
                    }
                # Default response for other dictionary results
                return {
                    "success": True,
                    "type": "result",
                    "query": natural_query,
                    "sql": query_result['sql'],
                    "data": result
                }
            else:
                return {
                    "success": False,
                    "error": "Invalid query result format",
                    "details": "The query parser returned an invalid format"
                }
        
        # Handle case where parse_simple_query returns a SQL string
        sql = query_result if isinstance(query_result, str) else str(query_result)
        result = execute_query(sql, query_result.get('params', {}) if hasattr(query_result, 'get') else {})
        
        # Format the response
        if isinstance(result, list):
            print(f"[DEBUG] Query successful, found {len(result)} rows")
            return {
                "success": True,
                "query": natural_query,
                "sql": sql,
                "data": result,
                "count": len(result)
            }
        else:
            print(f"[DEBUG] Non-query executed, affected {result.get('rowcount', 0)} rows")
            return {
                "success": True,
                "query": natural_query,
                "sql": sql,
                "affected_rows": result.get("rowcount", 0)
            }
            
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[ERROR] Query failed: {str(e)}")
        print(f"[ERROR] Traceback: {error_trace}")
        
        return {
            "success": False,
            "query": natural_query,
            "error": str(e),
            "sql": sql if 'sql' in locals() else "",
            "traceback": error_trace
        }
