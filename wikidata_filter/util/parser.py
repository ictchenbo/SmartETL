import ast
import re


def literal_eval(response_content: str):
    response_content = response_content.strip()

    # remove content between <think> and </think>, especial for DeepSeek reasoning model
    if "<think>" and "</think>" in response_content:
        end_of_think = response_content.find("</think>") + len("</think>")
        response_content = response_content[end_of_think:]

    try:
        if response_content.startswith("```") and response_content.endswith("```"):
            if response_content.startswith("```python"):
                response_content = response_content[9:-3]
            elif response_content.startswith("```json"):
                response_content = response_content[7:-3]
            elif response_content.startswith("```str"):
                response_content = response_content[6:-3]
            elif response_content.startswith("```\n"):
                response_content = response_content[4:-3]
            else:
                raise ValueError("Invalid code block format")
        result = ast.literal_eval(response_content.strip())
    except:
        matches = re.findall(r'(\[.*?\]|\{.*?\})', response_content, re.DOTALL)

        if len(matches) != 1:
            raise ValueError(f"Invalid JSON/List format for response content:\n{response_content}")

        json_part = matches[0]
        return ast.literal_eval(json_part)

    return result


def tag_split(text: str):
    if not text:
        return []
    if ':' in text:
        text = text[text.find(':') + 1:]
    if '：' in text:
        text = text[text.find('：') + 1:]
    parts = re.split('[;；。]+', text)
    return [p.strip() for p in parts]
