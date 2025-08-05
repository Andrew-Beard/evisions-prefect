import re
from canvas.utils import paginate, request_with_retry


def flatten_comments(comments):
    flat_list = []

    def recurse(comment):
        comment_copy = {k: v for k, v in comment.items() if k != "replies"}
        comment_copy["message"] = remove_html_tags(comment_copy["message"])
        flat_list.append(comment_copy)
        if "replies" in comment and comment["replies"]:
            for reply in comment["replies"]:
                recurse(reply)

    for comment in comments:
        recurse(comment)

    return flat_list


def remove_html_tags(text):
    clean = re.compile("<.*?>")
    return re.sub(clean, "", text)


def remove_decimal(value):
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def get_courses(client):
    url = f"{client.host_name_url}/api/v1/accounts/{client.account_id}/courses?per_page=100&state=available"
    headers = {"Authorization": f"Bearer {client.token}"}
    r = request_with_retry(client, url, headers)
    return paginate(client, r.json(), r, False)


def loop_courses(client, endpoint):
    courses = get_courses(client)
    result_list = []
    for course in courses:
        url = f"{client.host_name_url}/api/v1/courses/{str(course['id'])}{endpoint}?per_page=100"
        headers = {"Authorization": f"Bearer {client.token}"}
        r = request_with_retry(client, url, headers)
        list2 = r.json()
        list2 = paginate(client, list2, r, False)
        for item in list2:
            item["course_id"] = str(course["id"])
        result_list.extend(list2)
    return result_list


def get_course_enrollment(client):
    return loop_courses(client, "/users")


def get_discussion_topics(client):
    result_list = loop_courses(client, "/discussion_topics")
    for item in result_list:
        item["id"] = str(int(item["id"]))
    return result_list


def get_discussion_entries(client):
    topics = get_discussion_topics(client)
    result_list = []
    for item in topics:
        url = (
            f"{client.host_name_url}/api/v1/courses/{str(item['course_id'])}/discussion_topics/"
            f"{str(item['id'])}/view?per_page=100"
        )
        headers = {"Authorization": f"Bearer {client.token}"}
        r = request_with_retry(client, url, headers).json()
        normalized_structure = flatten_comments(r["view"])
        list2 = normalized_structure
        for item2 in list2:
            item2["course_id"] = item["course_id"]
            item2["topic_id"] = item["id"]
        result_list.extend(list2)
    return result_list


def get_users(client):

    url = f"{client.host_name_url}/api/v1/accounts/{client.account_id}/users?per_page=100"
    headers = {"Authorization": f"Bearer {client.token}"}
    r = request_with_retry(client, url, headers)
    result_list = r.json()
    result_list = paginate(client, result_list, r, False)
    for item in result_list:
        item["sis_import_id"] = remove_decimal(item["sis_import_id"])
    return result_list


def get_quizzes(client):
    result_list = loop_courses(client, "/quizzes")
    for item in result_list:
        item["id"] = remove_decimal(item["id"])
    return result_list


def get_quiz_submissions(client):
    quizzes = get_quizzes(client)
    result_list = []
    for item in quizzes:
        url = (
            f"{client.host_name_url}/api/v1/courses/{str(item['course_id'])}/quizzes/"
            f"{str(item['id'])}/submissions?per_page=100"
        )
        headers = {"Authorization": f"Bearer {client.token}"}
        r = request_with_retry(client, url, headers)
        list2 = r.json()["quiz_submissions"]
        list2 = paginate(client, list2, r, True)
        for item2 in result_list:
            item2["course_id"] = item["course_id"]
        result_list.extend(list2)
    return result_list


def get_assignments(client):
    result_list = loop_courses(client, "/assignments")
    for item in result_list:
        item["id"] = remove_decimal(item["id"])
        item["description"] = remove_html_tags(str(item["description"]))
    return result_list


def get_assignment_submissions(client):
    assignments = get_assignments(client)
    results_list = []
    for item in assignments:
        url = (
            f"{client.host_name_url}/api/v1/courses/{str(item['course_id'])}/assignments/"
            f"{str(item['id'])}/submissions?per_page=100"
        )
        headers = {"Authorization": f"Bearer {client.token}"}
        r = request_with_retry(client, url, headers)
        list2 = r.json()
        list2 = paginate(client, list2, r, False)
        for item2 in results_list:
            item2["course_id"] = item["course_id"]
        results_list.extend(list2)
    return results_list
