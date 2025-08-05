# from ayx_plugins.utilities.message_utils.messages import Message, MessageWrapper, Severity
import requests
import time

def request_with_retry(client, url, headers, max_retries=3, wait_time=5):

    retry_status_codes=(403, 500, 502, 503, 504)
    
    for attempt in range(max_retries):
        try:
            response = client.session.get(url, headers=headers, verify=False)  # Corrected line
            response.raise_for_status()
            return response  

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1 and (response is None or response.status_code in retry_status_codes):
                # MessageWrapper().display(Message(type="log", text=f"Request attempt failed: {requests.exceptions.RequestException}. Retrying in {wait_time} seconds...", severity=Severity.WARNING))
                # MessageWrapper().display(Message(type="log", text=f"(attempt {attempt + 1}/{max_retries}): {e}.", severity=Severity.WARNING))
                time.sleep(wait_time)
            else:
                # MessageWrapper().display(Message(type="log", text=f"Request failed after {max_retries} attempts: {e}. Exception: {requests.exceptions.RequestException}", severity=Severity.ERROR))
                return None

    return None


def paginate(client, base_data, request, quiz_submissions):
    result_list = base_data
    r = request
    
    while "link" in r.headers and 'rel="next"' in r.headers["link"]:
        split = r.headers["link"].split(",")
        for x in split:
            if 'rel="next"' in x:
                url = x.split(";")[0][1:-1]
                r = request_with_retry(client, url, headers={"Authorization": f"Bearer {client.token}"}, max_retries=3, wait_time=5)

                if quiz_submissions:
                    result_list.extend(r.json()["quiz_submissions"])
                else:
                    result_list.extend(r.json())

    return result_list
