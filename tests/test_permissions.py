import json
import os
import pwd
import shutil
import subprocess
import sys

from abx_dl.services.process_service import _hook_child_identity


def test_mixed_root_hook_identity_targets_effective_user():
    assert _hook_child_identity(real_uid=0, effective_uid=997, effective_gid=986) == (997, 986)
    assert _hook_child_identity(real_uid=501, effective_uid=501, effective_gid=20) is None


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
        "/bin/sh",
        "-c",
        "id -ru; id -u; id -rg; id -g; id -G",
        stdout=asyncio.subprocess.PIPE,
        preexec_fn=_permanently_drop_child_privileges(*identity),
    )
    stdout, _ = await child.communicate()
    assert child.returncode == 0
    values = stdout.decode().splitlines()
    print(json.dumps([*(int(value) for value in values[:4]), [int(value) for value in values[4].split()]]))

asyncio.run(main())
"""
    if os.geteuid() == 0:
        probe_command = [sys.executable, "-c", probe]
    else:
        sudo = shutil.which("sudo")
        assert sudo, "privilege-drop tests require sudo when not running as root"
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
