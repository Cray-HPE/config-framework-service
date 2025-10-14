#
# MIT License
#
# (C) Copyright 2019-2025 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

"""
Utilities for converting data between CFS v2 and v3 format
"""

from typing import Any

from cray.cfs.api.models.base_model import Model as BaseModel

from .typing import JsonData, JsonDict


def convert_data_to_v2(v3_data: JsonDict, v2_model_type: type[BaseModel]) -> JsonDict:
    """
    Field names in CFS v2 are in camelCase and in CFS v3 they are in snake_case.
    This function takes dictionary data in the CFS v3 format, and returns the CFS v2 equivalent.
    The specified model_type is the auto-generated model class for the CFS v2 schema for the
    data being converted. These model classes are generated at built time by the OpenAPI Generator.
    See the Dockerfile for details.

    When exporting from a model with to_dict, all keys are in snake_case.  However the model
    contains the information on the keys in the api spec.  This gives the ability to make the
    data match the given model/spec, which is useful when translating between the v2 and v3 api

    Data must start in the v3 format exported by model().to_dict()
    """
    v2_data = {}
    v2_model = v2_model_type()
    for attribute, attribute_key in v2_model.attribute_map.items():
        if attribute in v3_data:
            data_type = v2_model.openapi_types[attribute]
            v2_data[attribute_key] = _convert_data_to_v2(v3_data[attribute], data_type)
    return v2_data


def _convert_data_to_v2(v3_data: JsonData, data_type: Any) -> JsonData:
    if not isinstance(data_type, type):
        # Special case where the data_type is a "typing" object.  e.g typing.Dict
        if not v3_data:
            return v3_data
        if data_type.__origin__ == list:
            return [_convert_data_to_v2(item_data, data_type.__args__[0])
                    for item_data in v3_data]
        if data_type.__origin__ == dict:
            return {key: _convert_data_to_v2(item_data, data_type.__args__[1])
                    for key, item_data in v3_data.items()}
    elif issubclass(data_type, BaseModel):
        if not v3_data:
            v3_data = {}
        return convert_data_to_v2(v3_data, data_type)
    return v3_data


def convert_data_from_v2(v2_data: JsonDict, v2_model_type: type[BaseModel]) -> JsonDict:
    """
    Field names in CFS v2 are in camelCase and in CFS v3 they are in snake_case.
    This function takes dictionary data in the CFS v2 format, and returns the CFS v3 equivalent.
    The specified model_type is the auto-generated model class for the CFS v2 schema for the
    data being converted. These model classes are generated at built time by the OpenAPI Generator.
    See the Dockerfile for details.

    When exporting from a model with to_dict, all keys are in snake_case.  However the model
    contains the information on the keys in the api spec.  This gives the ability to make the
    data match the given model/spec, which is useful when translating between the v2 and v3 api

    Data must start in the v2 format exported by model().to_dict()
    """
    v3_data = {}
    v2_model = v2_model_type()
    for attribute_key, attribute in v2_model.attribute_map.items():
        if attribute in v2_data:
            data_type = v2_model.openapi_types[attribute_key]
            v3_data[attribute_key] = _convert_data_from_v2(v2_data[attribute], data_type)
    return v3_data


def _convert_data_from_v2(v2_data: JsonData, data_type: Any) -> JsonData:
    if not isinstance(data_type, type):
        # Special case where the data_type is a "typing" object.  e.g typing.Dict
        if not v2_data:
            return v2_data
        if data_type.__origin__ == list:
            return [_convert_data_from_v2(item_data, data_type.__args__[0])
                    for item_data in v2_data]
        if data_type.__origin__ == dict:
            return {key: _convert_data_from_v2(item_data, data_type.__args__[1])
                    for key, item_data in v2_data.items()}
    elif issubclass(data_type, BaseModel):
        if not v2_data:
            v2_data = {}
        return convert_data_from_v2(v2_data, data_type)
    return v2_data
