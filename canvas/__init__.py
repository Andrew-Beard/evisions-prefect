from . import config, client, endpoints

CanvasVersion = config.VERSION
APIClient = client.APIClient
CanvasProductId = config.PRODUCT_ID

ENDPOINTS = {
    "Users": endpoints.get_users,
    "Quizzes": endpoints.get_quizzes,
    "Quiz Submissions": endpoints.get_quiz_submissions,
    "Assignments": endpoints.get_assignments,
    "Assignment Submissions": endpoints.get_assignment_submissions,
    "Courses": endpoints.get_courses,
    "Course Enrollment": endpoints.get_course_enrollment,
    "Discussions": endpoints.get_discussion_topics,
    "Discussion Entries": endpoints.get_discussion_entries
}

