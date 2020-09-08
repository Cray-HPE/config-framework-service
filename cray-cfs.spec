# Copyright 2019 Cray Inc. All Rights Reserved.
Name: cray-cfs-crayctldeploy
License: Cray Software License Agreement
Summary: Cray Configuration Framework Service
Group: System/Management
Version: %(cat .rpm_version)
Release: %(echo ${BUILD_METADATA})
Source: %{name}-%{version}.tar.bz2
Vendor: Cray Inc.
Requires: cray-crayctl
Requires: cray-cmstools-crayctldeploy
Requires: kubernetes-crayctldeploy
Requires: cray-cfs-operator-crayctldeploy

# Project level defines TODO: These should be defined in a central location; DST-892
%define afd /opt/cray/crayctl/ansible_framework
%define modules %{afd}/library

%description
The Cray Configuration Framework Service (CFS) is responsible for managing
Ansible Execution Environments on Shasta L3 systems.

%prep
%setup -q

%build

%install
install -D -m 644 lib/cfs.py %{buildroot}%{modules}/cfs.py

# Install smoke tests under /opt/cray/tests/crayctl-stage4
mkdir -p ${RPM_BUILD_ROOT}/opt/cray/tests/crayctl-stage4/cms/
cp ct-tests/cfs_stage4_ct_tests.sh ${RPM_BUILD_ROOT}/opt/cray/tests/crayctl-stage4/cms/cfs_stage4_ct_tests.sh

%clean

%files
%defattr(755, root, root)
%dir %{modules}
%{modules}/cfs.py

/opt/cray/tests/crayctl-stage4/cms/cfs_stage4_ct_tests.sh

