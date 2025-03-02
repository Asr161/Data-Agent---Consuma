import json
import os
from dotenv import load_dotenv
import openai
from openai import OpenAI

# Load environment variables and initialize the OpenAI client
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai_api_key)

# Load the entire dataset from a JSON file
json_file_path = "sample_data.json"
with open(json_file_path, "r", encoding="utf-8") as f:
    entire_dataset = f.read()

half_data = entire_dataset[:len(entire_dataset)//16]
# Create a prompt that passes the entire dataset and instructs the LLM to output only a list
# of test question and answer pairs. Each pair should be in the format:
# Q: <test question>
# A: <expected answer>
prompt = f"""
You are an expert in designing automated tests for an ETL/Data Agent that ingests, transforms, and stores social media data from multiple platforms.
Below is the entire dataset used by the agent:

{half_data}

Based on this dataset, generate 10 test cases for the system. For each test case, output only a test question and the expected answer (that I can use in assertions).
The output format should be exactly as follows:

Test 1:
Q: <Test question>
A: <Expected answer>

Test 2:
Q: <Test question>
A: <Expected answer>

... and so on, for all 10 tests.

Only output the list of test cases, with no additional commentary.
"""

# Call the LLM
response = client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[{"role": "user", "content": prompt}],
    temperature=0.7,
    max_tokens=1000  # Adjust max_tokens as needed
)

# Print the generated test cases
print(response.choices[0].message.content)
