"""Export the committed tree as a private, history-free Git release bundle.

Unlike git archive, this includes tracked documentation and tests even when
legacy export-ignore attributes exclude them. Ignored machine files are absent.
Review committed credentials before publishing; this is not a secret scrubber.
"""
import argparse
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--previous", type=Path, help="Previous clean release bundle; preserves fast-forward deployment history")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    if output.exists():
        parser.error("Output already exists; choose a new release bundle path")
    source = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    entries = subprocess.check_output(["git", "-C", str(root), "ls-tree", "-rz", "--full-tree", "HEAD"])
    records = []
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, object_id = metadata.split()
        if kind != b"blob" or mode not in (b"100644", b"100755"):
            raise ValueError("Only regular tracked source files are supported")
        records.append((mode, object_id, name.decode("utf-8")))
    blobs = subprocess.check_output(
        ["git", "-C", str(root), "cat-file", "--batch"],
        input=b"".join(object_id+b"\n" for _, object_id, _ in records),
    )
    with tempfile.TemporaryDirectory(prefix="alpha-release-") as directory:
        stage = Path(directory)
        cursor = 0
        for mode, _, name in records:
            end = blobs.index(b"\n", cursor)
            size = int(blobs[cursor:end].split()[2])
            start = end + 1
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(blobs[start:start+size])
            if mode == b"100755":
                target.chmod(0o755)
            cursor = start + size + 1
        def git(*arguments):
            subprocess.run(["git", "-C", str(stage), *arguments], check=True)
        git("init", "--initial-branch=main", "--quiet")
        git("config", "user.name", "Alpha release")
        git("config", "user.email", "release@localhost")
        git("config", "core.autocrlf", "false")
        if args.previous:
            git("fetch", "--quiet", str(args.previous.resolve()), "main:refs/heads/main")
            git("reset", "--mixed", "--quiet", "main")
        git("add", "--all")
        git("commit", "--quiet", "--allow-empty", "-m", f"release(alpha): import source main {source}")
        git("bundle", "create", str(output), "main")
    print(f"Exported {len(records)} tracked files from {source}; bundle contains release commits only, no source history.")


if __name__ == "__main__":
    main()
