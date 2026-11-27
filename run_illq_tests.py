"""Run pytest via subprocess and save results to a text file (bypasses conda GBK issue)."""
import subprocess, sys, os
result = subprocess.run(
    [sys.executable, "-m", "pytest", "tests/test_module_illq.py", "--tb=short", "-v"],
    capture_output=True,
    cwd=r"D:\alpha",
    env={**os.environ, "PYTHONIOENCODING": "utf-8"},
)
out = (result.stdout + result.stderr).decode("utf-8", errors="replace")
with open(r"D:\alpha\illq_results2.txt", "w", encoding="utf-8") as f:
    f.write(out)
    f.write(f"\nExit code: {result.returncode}\n")
print("Done, exit:", result.returncode)
# Print last 50 lines so we see summary
lines = out.splitlines()
for line in lines[-50:]:
    print(line)
