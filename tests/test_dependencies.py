import asyncio
import subprocess
from pathlib import Path

from abxpkg.binary_service import BinaryService

from abx_dl.dependencies import resolve_binary_requests
from abx_dl.orchestrator import create_bus


def test_resolve_binary_requests_uses_real_abxpkg_env_projection(tmp_path: Path) -> None:
    lib_dir = tmp_path / "lib"
    bus = create_bus(name=f"dependencies_real_python_{tmp_path.name}")
    BinaryService(bus, auto_install=False, lib_dir=lib_dir)

    async def run():
        try:
            return await resolve_binary_requests(
                bus,
                {
                    "python3": {
                        "name": "python3",
                        "binproviders": "env",
                    },
                    "bash": {
                        "name": "bash",
                        "binproviders": "env",
                    },
                },
            )
        finally:
            await bus.wait_until_idle()
            await bus.destroy(clear=False)

    resolved = asyncio.run(run())
    python_event = resolved["python3"]
    shell_event = resolved["bash"]

    assert python_event is not None
    assert python_event.name == "python3"
    assert python_event.binprovider == "env"
    assert Path(python_event.abspath) == lib_dir / "env" / "bin" / "python3"
    assert Path(python_event.abspath).is_symlink()
    result = subprocess.run(
        [python_event.abspath, "-c", "print('canonical-abxpkg')"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout == "canonical-abxpkg\n"
    assert shell_event is not None
    assert shell_event.name == "bash"
    assert shell_event.binprovider == "env"
    assert Path(shell_event.abspath) == lib_dir / "env" / "bin" / "bash"
    assert Path(shell_event.abspath).is_symlink()
