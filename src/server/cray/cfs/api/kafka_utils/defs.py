#
# MIT License
#
# (C) Copyright 2020-2026 Hewlett Packard Enterprise Development LP
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

from cray.cfs.api.env_utils import get_pos_float_env_var_or_default

DEFAULT_KAFKA_FLUSH_TIMEOUT = 3

KAFKA_PORT = '9092'
KAFKA_K8S_SERVICE_NAME = "cray-shared-kafka-kafka-bootstrap"
KAFKA_K8S_SERVICE_NS = "services"

# Allow a different flush timeout value to be specified
KAFKA_FLUSH_TIMEOUT = get_pos_float_env_var_or_default('KAFKA_PRODUCER_FLUSH_TIMEOUT',
                                                       DEFAULT_KAFKA_FLUSH_TIMEOUT)
