from fastapi import FastAPI, Request
from dotenv import load_dotenv
import google.generativeai as genai
import os
from db import *
from bson import json_util
import json

load_dotenv()
app = FastAPI()

genai.configure(api_key="AIzaSyAHY511CZEid3HR4V7ljTpnwd7ARkF1bW0")
model = genai.GenerativeModel("gemini-2.0-flash")

@app.post("/query")
async def query_natural_language(request: Request):
    data = await request.json()
    user_query = data.get("query")

    # Step 3: Run SQL and return results
    try:
        conn = get_connection()
        schema = get_sql_server_schema(conn)
        classify_prompt = f"""
        You are a helpful assistant. 
        Given the following user query, classify it as either 'data' (if it requests data from the database) or 'explanation' (if it asks for a description, schema, or explanation).
        Only reply with 'data' or 'explanation'.

        User query: "{user_query}"
        """
        classify_response = model.generate_content(classify_prompt)
        query_type = classify_response.text.strip().lower()
        dbName = os.getenv("DB_NAME")

        if query_type == "data":
            prompt = f"""You are a helpful assistant that converts natural language to SQL to be executed in SQL Server Management Studio.
                    Based on the following schema:

                    {schema}

                    Write an SQL query for queries asked in natural language.
                    Do not include explanations.

                    the query is: "{user_query}"
                    """

            response = model.generate_content(prompt)
            sql_query = response.text.strip().strip("```sql").strip("```")

            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            conn.close()

            return {
                "generated_sql": sql_query,
                "results": results
            }
            
        else:
            explain_prompt = f"""You are a helpful assistant that explains SQL schemas and data organization.
            Based on the following Sql schema and sample documents from the {dbName} dataset:

            {schema}

            Answer the following user question in detail, but do not generate code:

            "{user_query}"
            """
            gemini_response = model.generate_content(explain_prompt)
            return {
                "explanation": gemini_response.text.strip()
            }
    except Exception as e:
        return {
            "error": str(e),
            "generated_sql": sql_query
        }


@app.post("/nosql-query")
async def query_nosql(request: Request):
    data = await request.json()
    user_query = data.get("query")
    pymongo_code = ""

    try:
        # TODO Implement security checks
        db = get_mongo_connection()
        schema = get_mongo_schema(db)
        dbName = os.getenv("MONGO_DB_NAME")

        classify_prompt = f"""
        You are a helpful assistant. 
        Given the following user query, classify it as either 'data' (if it requests data from the database) or 'explanation' (if it asks for a description, schema, or explanation).
        Only reply with 'data' or 'explanation'.

        User query: "{user_query}"
        """
        classify_response = model.generate_content(classify_prompt)
        query_type = classify_response.text.strip().lower()

        if query_type == "data":
            prompt = f"""You are a helpful assistant that converts natural language to MongoDB queries.
            Based on the following NoSQL schema and sample documents from the {dbName} dataset:

            {schema}

            Convert the following user query into a MongoDB query (pymongo format). 
            Use 'result = db.<collection>.find(...)' syntax.
            Do not include explanations or comments. Just return valid Python code.

            The user query is: "{user_query}"
            """

            gemini_response = model.generate_content(prompt)
            pymongo_code = gemini_response.text.strip().strip("```python").strip("```")
            print(pymongo_code)
            exec_locals = {"db": db}
            exec(pymongo_code, {}, exec_locals)
            result = exec_locals.get("result")
            if result and not isinstance(result, list):
                result = list(result)

            # Serialize to JSON-safe format
            safe_result = json.loads(json_util.dumps(result))

            return {
                "query": pymongo_code,
                "result": safe_result
            }
        else:
            explain_prompt = f"""You are a helpful assistant that explains MongoDB schemas and data organization.
            Based on the following NoSQL schema and sample documents from the {dbName} dataset:

            {schema}

            Answer the following user question in detail, but do not generate code:

            "{user_query}"
            """
            gemini_response = model.generate_content(explain_prompt)
            return {
                "explanation": gemini_response.text.strip()
            }
    except Exception as e:
        return {"error": str(e), "generated_query": pymongo_code}
