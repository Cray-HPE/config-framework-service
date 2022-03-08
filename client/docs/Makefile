#
# MIT License
#
# (C) Copyright 2021-2022 Hewlett Packard Enterprise Development LP
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
SDIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/../../api)
ODIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))


all: checkout build

checkout:

build:
	echo $(DIR)
	docker run --rm -v $(SDIR)/:/api -v $(ODIR):/build create-adoc convert -i /api/openapi.yaml -d /build
	docker run -v $(DIR):/docs -v $(ODIR):/build create-html --doctype book /docs/index.adoc  -D /build
	docker run -v $(DIR):/docs -v $(ODIR):/build create-pdf  --doctype book /docs/index.adoc -D /build

clean:
	rm *.pdf *.html overview.adoc security.adoc definitions.adoc paths.adoc 
