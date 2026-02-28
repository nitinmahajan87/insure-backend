import xmltodict
from xml.parsers.expat import ExpatError
from fastapi import Request, HTTPException


def clean_xml_dict(data):
    """
    Recursively removes XML namespaces (e.g., 'ns1:EmpID' -> 'EmpID')
    and ignores XML attributes (keys starting with '@').
    """
    if isinstance(data, dict):
        clean_dict = {}
        for k, v in data.items():
            # Ignore XML attributes entirely
            if k.startswith('@'):
                continue

            # Strip namespace prefix if it exists
            clean_key = k.split(':')[-1] if ':' in k else k
            clean_dict[clean_key] = clean_xml_dict(v)
        return clean_dict
    elif isinstance(data, list):
        return [clean_xml_dict(item) for item in data]
    else:
        return data


async def universal_payload_parser(request: Request) -> dict:
    """
    Reads the incoming request, checks the Content-Type,
    and translates either JSON or XML into a standard Python dictionary.
    """
    content_type = request.headers.get("content-type", "").lower()

    # --- PATH 1: JSON ---
    if "application/json" in content_type:
        try:
            return await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # --- PATH 2: XML ---
    elif "application/xml" in content_type or "text/xml" in content_type:
        raw_body = await request.body()

        if not raw_body or not raw_body.strip():
            raise HTTPException(status_code=400, detail="Empty XML payload")

        try:
            parsed_xml = xmltodict.parse(raw_body)

            if not parsed_xml:
                return {}

            root_key = list(parsed_xml.keys())[0]
            data_dict = parsed_xml[root_key]

            # 1. FIX: Ensure the payload is an object, not just a string
            if not isinstance(data_dict, dict):
                return {}

            # 2. FIX: Clean the dictionary of namespaces and attributes
            clean_dict = clean_xml_dict(data_dict)

            # 3. FIX: Handle nested wrappers (e.g., <Event><Employee>...</Employee></Event>)
            # If the root element just wrapped one single object, unwrap it one more level.
            if len(clean_dict) == 1 and isinstance(list(clean_dict.values())[0], dict):
                return list(clean_dict.values())[0]

            return clean_dict

        except ExpatError as e:
            raise HTTPException(status_code=400, detail=f"Malformed XML: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing XML payload: {str(e)}")

    # --- PATH 3: Unsupported Format ---
    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported Media Type: '{content_type}'. Please send application/json or application/xml."
        )