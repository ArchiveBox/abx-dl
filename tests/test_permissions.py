import json
import os
import pwd
import shlex
import shutil
import subprocess
import sys

import pytest

from abx_dl.services.process_service import _hook_child_identity


def test_mixed_root_hook_identity_targets_effective_user():
    assert _hook_child_identity(real_uid=0, effective_uid=997, effective_gid=986) == (997, 986)
    assert _hook_child_identity(real_uid=501, effective_uid=501, effective_gid=20) is None


@pytest.mark.parametrize("child_path", [None, ""])
def test_hook_child_process_uses_expected_real_and_effective_ids(child_path):
    id_binary = shutil.which("id", path=os.defpath)
    assert id_binary is not None
    id_command = shlex.quote(id_binary)
    nobody = pwd.getpwnam("nobody")
    running_as_root = os.geteuid() == 0
    identity_setup = f"os.setegid({nobody.pw_gid})\nos.seteuid({nobody.pw_uid})" if running_as_root else ""
    expected_identity = f"({nobody.pw_uid}, {nobody.pw_gid})" if running_as_root else "None"
    probe = f"""
import asyncio
import json
import os

from abx_dl.services.process_service import _hook_child_identity, _permanently_drop_child_privileges

{identity_setup}
identity = _hook_child_identity()
assert identity == {expected_identity}

async def main():
    child = await asyncio.create_subprocess_exec(
        "/bin/sh",
        "-c",
        {f"{id_command} -ru; {id_command} -u; {id_command} -rg; {id_command} -g; {id_command} -G"!r},
        stdout=asyncio.subprocess.PIPE,
        preexec_fn=_permanently_drop_child_privileges(*identity) if identity else None,
    )
    stdout, _ = await child.communicate()
    assert child.returncode == 0
    values = stdout.decode().splitlines()
    print(json.dumps([*(int(value) for value in values[:4]), [int(value) for value in values[4].split()]]))

asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=os.getcwd(),
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "PATH": child_path} if child_path is not None else None,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    real_uid, effective_uid, real_gid, effective_gid, groups = json.loads(result.stdout)
    expected_uid = nobody.pw_uid if running_as_root else os.getuid()
    expected_gid = nobody.pw_gid if running_as_root else os.getgid()
    assert (real_uid, effective_uid, real_gid, effective_gid) == (
        expected_uid,
        expected_uid,
        expected_gid,
        expected_gid,
    )
    if running_as_root:
        assert groups == [expected_gid]
    else:
        assert expected_gid in groups
