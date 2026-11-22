import os
import sys

def report(log_file):
    if not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        return
    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    print("\n=======================================================")
    print(f" C++ BUILD LOG: {log_file}")
    print("=======================================================\n")
    print(content[-5000:] if len(content) > 5000 else content)

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        try:
            with open(summary_file, "a", encoding="utf-8") as out:
                out.write(f"\n### ❌ C++ Build Failure: `{log_file}`\n\n```\n")
                out.write(content[-3000:] if len(content) > 3000 else content)
                out.write("\n```\n")
        except Exception as e:
            print(f"Error writing step summary: {e}")

if __name__ == "__main__":
    log_name = sys.argv[1] if len(sys.argv) > 1 else "build_x86.log"
    report(log_name)
