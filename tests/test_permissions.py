import json
import os
import pwd
import shutil
import subprocess
import sys

import pytest

from abx_dl.services.process_service import _hook_child_identity


def test_mixed_root_hook_identity_targets_effective_user():
    assert _hook_child_identity(real_uid=0, effective_uid=997, effective_gid=986) == (997, 986)
    assert _hook_child_identity(real_uid=501, effective_uid=501, effective_gid=20) is None


@pytest.mark.skipif(sys.platform != "linux", reason="Linux root credential regression")
def test_mixed_root_hook_child_permanently_drops_real_and_effective_ids():
    nobody = pwd.getpwnam("nobody")
    probe = f"""
import asyncio
import json
import os
import sys

from abx_dl.services.process_service import _hook_child_identity, _permanently_drop_child_privileges

os.setegid({nobody.pw_gid})
os.seteuid({nobody.pw_uid})
identity = _hook_child_identity()
assert identity == ({nobody.pw_uid}, {nobody.pw_gid})

async def main():
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import json, os; print(json.dumps([os.getuid(), os.geteuid(), os.getgid(), os.getegid(), os.getgroups()]))",
        stdout=asyncio.subprocess.PIPE,
        preexec_fn=_permanently_drop_child_privileges(*identity),
    )
    stdout, _ = await child.communicate()
    assert child.returncode == 0
    print(stdout.decode().strip())

asyncio.run(main())
"""
    if os.geteuid() == 0:
        probe_command = [sys.executable, "-c", probe]
    else:
        sudo = shutil.which("sudo")
        assert sudo, "Ubuntu CI must provide sudo"
        probe_command = [sudo, "-n", sys.executable, "-c", probe]

    result = subprocess.run(
        probe_command,
        cwd=os.getcwd(),
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert json.loads(result.stdout) == [nobody.pw_uid, nobody.pw_uid, nobody.pw_gid, nobody.pw_gid, [nobody.pw_gid]]
