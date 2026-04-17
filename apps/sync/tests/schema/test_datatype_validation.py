import unittest
from database.schema import DATATYPE_CHECK
from typing import Any
from wywy_website_types import Datatype


def validation_function_positive_tests(
    test_object: unittest.TestCase, valid_values: list[Any], datatype: Datatype
):
    validation_function = DATATYPE_CHECK[datatype]
    for valid_value in valid_values:
        test_object.assertTrue(
            validation_function(valid_value),
            f"Valid value {valid_value} erroneously did not pass {datatype} datatype validation.",
        )


def validation_function_negative_tests(
    test_object: unittest.TestCase, valid_values: list[Any], datatype: Datatype
):
    validation_function = DATATYPE_CHECK[datatype]
    for valid_value in valid_values:
        test_object.assertFalse(
            validation_function(valid_value),
            f"Valid value {valid_value} erroneously passed {datatype} datatype validation.",
        )


class TestDatatypeValidation(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def geodetic_point_validation(self):
        validation_function_positive_tests(
            self,
            [
                "POINT (0.2325 0.2325)",
                "POINT (23 23)",
                "POINT (-0.2325 0.2325)",
                "POINT (0.2325 -0.2325)",
                "POINT (-0.2325 -0.2325)",
                "POINT (0.2525 0.2325)",
                "POINT (0.2325 0.2525)",
                # "'POINT(0.23 0.23)'",
                # "'POINT(23 23)'",
                # "'POINT(-0.23 0.23)'",
                # "'POINT(0.23 -0.23)'",
                # "'POINT(-0.23 -0.23)'",
                # "'POINT(0.25 0.23)'",
                # "'POINT(0.23 0.25)'",
            ],
            "geodetic point",
        )
        validation_function_negative_tests(
            self,
            [
                "'POINT (0.2325 0.2325)'",
                "POINTZ (0.2325 0.2325 0.2325)",
                "POINT (0. 2325 0.2325)",
                "POINT (0 .2325 0.2325)",
                "POINT (181 0.2325)",
                "POINT (180.00001 0.2325)",
                "POINT (-181 0.2325)",
                "POINT (-180.00001 0.2325)",
                "POINT (0.2325 91)",
                "POINT (0.2325 90.0001)",
                "POINT (0.2325 -91)",
                "POINT (0.2325 -90.0001)",
            ],
            "geodetic point",
        )
        print("DONE!!!)")

    # @TODO literally everything
