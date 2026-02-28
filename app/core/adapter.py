import xmltodict
from xml.parsers.expat import ExpatError
from fastapi import Request, HTTPException


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
            # xmltodict converts XML tags directly into dictionary keys
            parsed_xml = xmltodict.parse(raw_body)

            if not parsed_xml:
                return {}  # Return empty dict if XML had no meaningful nodes

            # XML always has a "Root" tag.
            # We want the data *inside* the root tag so it matches our Pydantic model.
            root_key = list(parsed_xml.keys())[0]
            data_dict = parsed_xml[root_key]

            # If the root tag was empty (e.g. <EmployeeEvent/>), data_dict might be None.
            return data_dict if data_dict is not None else {}

        except ExpatError as e:
            # Explicitly catch XML syntax errors
            raise HTTPException(status_code=400, detail=f"Malformed XML: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing XML payload: {str(e)}")

    # --- PATH 3: Unsupported Format ---
    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported Media Type: '{content_type}'. Please send application/json or application/xml."
        )