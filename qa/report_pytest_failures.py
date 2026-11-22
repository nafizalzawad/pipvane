import os
import sys
import xml.etree.ElementTree as ET

def report_failures(xml_path="pytest-results.xml"):
    if not os.path.exists(xml_path):
        print(f"File not found: {xml_path}")
        return

    tree = ET.parse(xml_path)
    root = tree.getroot()
    failures = []

    for tc in root.iter("testcase"):
        classname = tc.attrib.get("classname", "")
        name = tc.attrib.get("name", "")
        for f in tc.iter("failure"):
            msg = f.attrib.get("message", "") or (f.text[:200] if f.text else "")
            failures.append((f"{classname}.{name}", msg))
        for e in tc.iter("error"):
            msg = e.attrib.get("message", "") or (e.text[:200] if e.text else "")
            failures.append((f"{classname}.{name} [ERROR]", msg))

    print(f"\n=======================================================")
    print(f" TOTAL PYTEST FAILURES DETECTED: {len(failures)}")
    print(f"=======================================================\n")
    for test, msg in failures:
        print(f"FAIL: {test}\n  -> {msg}\n")

    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        try:
            with open(summary_file, "a", encoding="utf-8") as out:
                out.write(f"\n### ❌ Pytest Failures ({len(failures)})\n\n")
                for test, msg in failures:
                    out.write(f"- **`{test}`**\n  ```\n  {msg}\n  ```\n")
        except Exception as ex:
            print(f"Could not write to step summary: {ex}")

if __name__ == "__main__":
    xml_file = sys.argv[1] if len(sys.argv) > 1 else "pytest-results.xml"
    report_failures(xml_file)
