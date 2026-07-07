# Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
# This file is part of Async HBase.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#   - Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   - Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   - Neither the name of the StumbleUpon nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# The full PowerMock framework was dropped; only powermock-reflect (a standalone
# reflection helper, works on JDK 8-17) is still used -- see pom.xml.in. The old
# powermock-mockito-release-full "-full" assembly jar was never published for the
# 2.x line, so fetch powermock-reflect here to match the POM dependency. URL moved
# to repo1.maven.org (central.maven.org was retired).
POWERMOCK_MOCKITO_VERSION := 2.0.9
POWERMOCK_MOCKITO := third_party/powermock/powermock-reflect-$(POWERMOCK_MOCKITO_VERSION).jar
POWERMOCK_MOCKITO_BASE_URL := $(ASYNCHBASE_THIRD_PARTY_BASE_URL)/org/powermock/powermock-reflect/$(POWERMOCK_MOCKITO_VERSION)

$(POWERMOCK_MOCKITO): $(POWERMOCK_MOCKITO).md5
	set dummy "$(POWERMOCK_MOCKITO_BASE_URL)" "$(POWERMOCK_MOCKITO)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(POWERMOCK_MOCKITO)
