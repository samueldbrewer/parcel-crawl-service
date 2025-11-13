#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox

import requests
import urllib3
from requests import exceptions as req_exc

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL = "https://landlens.up.railway.app/files"
UPLOAD_TIMEOUT = int(os.getenv("UPLOAD_TIMEOUT", "900"))


def upload_file(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            response = requests.post(
                API_URL,
                files={"file": (path.name, handle)},
                timeout=UPLOAD_TIMEOUT,
                verify=False,
            )
        response.raise_for_status()
        return response.json()
    except req_exc.SSLError:
        return _upload_with_curl(path)


def _upload_with_curl(path: Path) -> dict:
    try:
        completed = subprocess.run(
            [
                "curl",
                "-sS",
                "-v",
                "-X",
                "POST",
                API_URL,
                "-k",
                "--max-time",
                str(UPLOAD_TIMEOUT),
                "-F",
                f"file=@{path}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - depends on CLI environment
        raise RuntimeError(exc.stderr or "curl upload failed.") from exc
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        detail = "\n".join(
            part for part in (completed.stdout.strip(), completed.stderr.strip()) if part
        ) or "empty response"
        raise RuntimeError(f"Unexpected response:\n{detail}") from exc


def main() -> None:
    root = tk.Tk()
    root.title("LandLens File Uploader")
    root.geometry("460x240")
    root.resizable(False, False)

    status_var = tk.StringVar(value="Pick a file to upload.")

    result_box = tk.Text(root, height=9, width=64, state="disabled")
    result_box.pack(padx=14, pady=(12, 6))

    def on_upload() -> None:
        selected = filedialog.askopenfilename(title="Select file to upload")
        if not selected:
            return
        path = Path(selected)
        status_var.set(f"Uploading {path.name} …")
        root.update_idletasks()
        try:
            payload = upload_file(path)
        except requests.RequestException as exc:
            messagebox.showerror("Upload failed", str(exc), parent=root)
            status_var.set("Upload failed.")
            return
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Upload failed", str(exc), parent=root)
            status_var.set("Upload failed.")
            return
        status_var.set("Upload successful!")
        result_box.configure(state="normal")
        result_box.delete("1.0", tk.END)
        result_box.insert(tk.END, json.dumps(payload, indent=2))
        result_box.configure(state="disabled")

    upload_btn = tk.Button(root, text="Select & Upload File", command=on_upload)
    upload_btn.pack(pady=4)

    status_label = tk.Label(root, textvariable=status_var, anchor="w")
    status_label.pack(fill="x", padx=14, pady=(0, 10))

    root.mainloop()


if __name__ == "__main__":
    main()
