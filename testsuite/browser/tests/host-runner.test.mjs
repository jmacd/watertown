import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const runner = fileURLToPath(new URL("../../run-test.sh", import.meta.url));

test("host tests receive no-rebuild and fail before execution if the image is missing", () => {
  const dir = mkdtempSync(join(tmpdir(), "watertown-host-runner-"));
  try {
    const runtime = join(dir, "container");
    const script = join(dir, "host.sh");
    writeFileSync(runtime, `#!/bin/bash
set -eu
[[ "$1 $2" == "image inspect" ]] || exit 92
exit "\${IMAGE_STATUS:-0}"
`, { mode: 0o755 });
    writeFileSync(join(dir, "timeout"), `#!/bin/bash
shift
exec "$@"
`, { mode: 0o755 });
    writeFileSync(script, `#!/bin/bash
# REQUIRES: host
set -eu
[[ "$#" == 1 && "$1" == "--no-rebuild" ]]
echo "host received no-rebuild"
`);
    const env = {
      ...process.env,
      PATH: `${dir}:${process.env.PATH}`,
      CONTAINER_RT: runtime,
      IMAGE_STATUS: "0",
    };
    for (const flag of ["--no-rebuild", "-n"]) {
      const result = spawnSync("bash", [runner, flag, script], { env, encoding: "utf8" });
      assert.equal(result.status, 0, result.stdout + result.stderr);
      assert.match(result.stdout, /host received no-rebuild/);
    }
    const missing = spawnSync("bash", [runner, "--no-rebuild", script], {
      env: { ...env, IMAGE_STATUS: "1" },
      encoding: "utf8",
    });
    assert.notEqual(missing.status, 0);
    assert.match(missing.stdout, /Image watertown-test:latest not found/);
    assert.doesNotMatch(missing.stdout, /host received no-rebuild/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
