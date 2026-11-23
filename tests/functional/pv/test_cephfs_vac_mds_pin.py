"""Tests for CephFS MDS export pinning applied through a VolumeAttributesClass."""

import base64
import json
import logging
import os
import shlex
import uuid

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
    skipif_cephfs_disabled,
    skipif_external_mode,
    skipif_hci_client,
    skipif_managed_service,
    skipif_ocp_version,
    skipif_ocs_version,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    adjust_active_mds_count_storagecluster,
    get_active_mds_count_cephfilesystem,
    get_mds_counts,
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import Pod, get_ceph_tools_pod
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check

logger = logging.getLogger(__name__)

VAC_NAME_PREFIX = "cephfs-mds-pin"
LIBCEPHFS_PIN_SCRIPT = """
import sys
import cephfs

fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.mount()
try:
    print("PIN=%s" % fs.getxattr(sys.argv[1], "ceph.dir.pin").decode())
finally:
    fs.shutdown()
"""


def get_dir_pin(dm_pod, path):
    """Returns ceph.dir.pin (-1 when unset) for a CephFS-relative path."""
    encoded = base64.b64encode(LIBCEPHFS_PIN_SCRIPT.encode()).decode()
    command = f"echo {encoded} | base64 -d | python3 - {shlex.quote(path)}"
    output = dm_pod.exec_sh_cmd_on_pod(command=command)
    return int(output.strip().split("PIN=")[-1])


def wait_for_dir_pin(dm_pod, path, expected, timeout=180):
    TimeoutSampler(timeout, 10, get_dir_pin, dm_pod, path).wait_for_func_value(expected)


def create_direct_mount_pod():
    """Creates a privileged pod that inherits the Ceph tools client configuration."""
    tools_spec = get_ceph_tools_pod().get()["spec"]
    tools_container = tools_spec["containers"][0]
    container = {
        "name": "direct-mount",
        "image": tools_container["image"],
        "command": tools_container["command"],
        "args": tools_container.get("args", []),
        "env": tools_container.get("env", []),
        "volumeMounts": tools_container.get("volumeMounts", []),
        "securityContext": {"privileged": True},
        "tty": True,
    }
    pod_dict = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"cephfs-vac-dm-{uuid.uuid4().hex[:8]}",
            "namespace": config.ENV_DATA["cluster_namespace"],
            "labels": {"app": "cephfs-vac-mds-pin"},
        },
        "spec": {
            "containers": [container],
            "volumes": tools_spec.get("volumes", []),
            "hostNetwork": True,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "restartPolicy": "Never",
        },
    }
    if tools_spec.get("serviceAccountName"):
        pod_dict["spec"]["serviceAccountName"] = tools_spec["serviceAccountName"]
    pod_obj = Pod(**pod_dict)
    pod_obj.create()
    return pod_obj


def wait_for_ceph_conf(dm_pod, timeout=120):
    def conf_exists():
        return "ceph.conf" in dm_pod.exec_sh_cmd_on_pod(command="ls /etc/ceph")

    TimeoutSampler(timeout, 5, conf_exists).wait_for_func_value(True)


def wait_for_vac_applied(pvc_obj, vac_name, timeout=300):
    def applied():
        status = pvc_obj.get().get("status", {})
        return status.get(
            "currentVolumeAttributesClassName"
        ) == vac_name and not status.get("modifyVolumeStatus")

    TimeoutSampler(timeout, 5, applied).wait_for_func_value(True)


def get_subvolume_base_path(pvc_obj, toolbox):
    """Returns the subvolume base path, which is where ceph.dir.pin is set."""
    attrs = pvc_obj.backed_pv_obj.get()["spec"]["csi"]["volumeAttributes"]
    if attrs.get("subvolumePath"):
        return os.path.dirname(attrs["subvolumePath"])
    fs_name = attrs.get("fsName") or helpers.get_cephfs_name()
    group = helpers.get_cephfs_subvolumegroup()
    cmd = (
        f"ceph fs subvolume getpath {fs_name} "
        f"{pvc_obj.get_cephfs_subvolume_name} {group}"
    )
    path = toolbox.exec_ceph_cmd(ceph_cmd=cmd, format=None, out_yaml_format=False)
    return os.path.dirname(path.strip())


@green_squad
@tier2
@skipif_ocp_version("<4.21")
@skipif_ocs_version("<4.21")
@skipif_external_mode
@skipif_hci_client
@skipif_managed_service
@skipif_cephfs_disabled
@polarion_id("OCS-XXXX")  # TODO: replace with the real Polarion ID
class TestCephFSVACMDSPin(ManageTest):
    """Tests CephFS MDS export pinning through a VolumeAttributesClass."""

    @pytest.fixture(autouse=True)
    def setup_two_active_mds(self, request):
        """Ensures two active MDS daemons and restores the original count."""
        original = get_active_mds_count_cephfilesystem()

        def active_count():
            return get_mds_counts()[0]

        def restore():
            if original < 2:
                adjust_active_mds_count_storagecluster(original)
                TimeoutSampler(300, 10, active_count).wait_for_func_value(original)
            ceph_health_check()

        request.addfinalizer(restore)
        if original < 2:
            logger.info(f"Scaling active MDS count from {original} to 2")
            adjust_active_mds_count_storagecluster(2)
            TimeoutSampler(300, 10, active_count).wait_for_func_value(2)

    def test_cephfs_mds_export_pin_via_vac(
        self, project_factory, pvc_factory, teardown_factory
    ):
        """Verifies that patching a CephFS PVC VAC updates ceph.dir.pin."""
        logger.test_step("Deploy direct-mount pod and mount the CephFS root")
        dm_pod = create_direct_mount_pod()
        teardown_factory(dm_pod)
        helpers.wait_for_resource_state(dm_pod, constants.STATUS_RUNNING, timeout=420)
        wait_for_ceph_conf(dm_pod)
        logger.info(f"CephFS root ceph.dir.pin: {get_dir_pin(dm_pod, '/')}")
        toolbox = get_ceph_tools_pod()

        logger.test_step("Create a CephFS PVC with no VAC and check ceph.dir.pin")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=project_factory(),
            size=5,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        base_path = get_subvolume_base_path(pvc_obj, toolbox)
        assert get_dir_pin(dm_pod, base_path) == -1, f"No MDS pin on {base_path}"

        logger.test_step("Create VACs exporting MDS rank 0 and rank 1")
        vac_objs = []
        for rank in (0, 1):
            vac_obj = OCS(
                apiVersion="storage.k8s.io/v1",
                kind="VolumeAttributesClass",
                metadata={"name": f"{VAC_NAME_PREFIX}-{rank}"},
                driverName=constants.CEPHFS_PROVISIONER,
                parameters={"mds-pin-export": str(rank)},
            )
            vac_obj.create()
            teardown_factory(vac_obj)
            vac_objs.append(vac_obj)
        vac_zero, vac_one = vac_objs
        # teardown_factory deletes in reverse order, so registering the PVC last
        # removes it before the VACs and unblocks their vac-protection finalizer.
        teardown_factory(pvc_obj)

        for vac_obj, rank in ((vac_zero, 0), (vac_one, 1)):
            logger.test_step(f"Patch PVC {pvc_obj.name} with VAC {vac_obj.name}")
            pvc_obj.ocp.patch(
                resource_name=pvc_obj.name,
                params=json.dumps(
                    {"spec": {"volumeAttributesClassName": vac_obj.name}}
                ),
                format_type="merge",
            )
            wait_for_vac_applied(pvc_obj, vac_obj.name)
            wait_for_dir_pin(dm_pod, base_path, rank)
