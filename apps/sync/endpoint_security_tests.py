from copy import deepcopy
from unittest import TestCase
from requests import post as POST
from constants import GENERIC_REQUEST_PARAMS


def test_endpoint_security(test_object: TestCase, endpoint: str) -> None:
    # does the endpoint check the origin header?
    request_params = deepcopy(GENERIC_REQUEST_PARAMS)
    request_params["headers"] = None
    response = POST(endpoint, **request_params)
    test_object.assertEqual(
        response.status_code,
        403,
        f"Invalid (origin-less) request did not receive response with status 403: {response.status_code}: {response.text}",
    )

    # is the endpoint secured with authentication?
    request_params = deepcopy(GENERIC_REQUEST_PARAMS)
    del request_params["cookies"]["token"]
    response = POST(endpoint, **request_params)
    test_object.assertEqual(
        response.status_code,
        401,
        f"Unauthenticated request did not receive response with status 401: {response.status_code}: {response.text}",
    )
