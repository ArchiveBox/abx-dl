import asyncio
import subprocess
from pathlib import Path

from abxpkg.binary_service import BinaryService

from abx_dl.dependencies import resolve_binary_requests
from abx_dl.orchestrator import create_bus


def test_resolve_binary_requests_uses_real_abxpkg_env_projection(tmp_path: Path) -> None:
    lib_dir = tmp_path / "lib"
    bus = create_bus(name=f"dependencies_real_git_{tmp_path.name}")
    BinaryService(bus, auto_install=False, lib_dir=lib_dir)

    async def run():
        try:
            return await resolve_binary_requests(
                bus,
                {
                    "sh": {
                        "name": "sh",
                        "binproviders": "env",
                    },
                },
            )
        finally:
            await bus.wait_until_idle()
            await bus.destroy(clear=False)

    resolved = asyncio.run(run())
    shell_event = resolved["sh"]

    assert shell_event is not None
    assert shell_event.name == "sh"
    assert shell_event.binprovider == "env"
    assert Path(shell_event.abspath) == lib_dir / "env" / "bin" / "sh"
    assert Path(shell_event.abspath).is_symlink()
    result = subprocess.run(
        [shell_event.abspath, "-c", "printf canonical-abxpkg"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout == "canonical-abxpkg"
